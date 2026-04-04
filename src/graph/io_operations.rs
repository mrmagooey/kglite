// src/graph/io_operations.rs
//
// Versioned binary format for KnowledgeGraph persistence.
//
// File format v3 layout:
//   [0..4]    Magic: b"RGF\x03" (Rusty Graph Format, version 3)
//   [4..8]    core_data_version: u32 LE (tracks NodeData/EdgeData/Value changes)
//   [8..12]   metadata_length: u32 LE
//   [12..12+N]  JSON metadata (column schemas, section sizes, all config)
//   [section]  topology.zst — graph structure WITHOUT node properties
//   [section]  columns_<Type>.zst — one per node type, packed column data
//   [section]  embeddings.zst (optional)
//   [section]  timeseries.zst (optional)

use crate::graph::column_store::ColumnStore;
use crate::graph::reporting::OperationReports;
use crate::graph::schema::{
    CompositeIndexKey, ConnectionTypeInfo, CowSelection, DirGraph, EmbeddingStore, IndexKey,
    PropertyStorage, SaveMetadata, SchemaDefinition, SerdeDeserializeGuard, SerdeSerializeGuard,
    SpatialConfig, StringInterner, StripPropertiesGuard, TemporalConfig,
};
use crate::graph::timeseries::{NodeTimeseries, TimeseriesConfig};
use crate::graph::{KnowledgeGraph, TemporalContext};
use bincode::Options;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::sync::Arc;

/// Return a pinned bincode configuration that is identical to the legacy
/// `bincode::serialize` / `bincode::deserialize` encoding:
///   - Fixed-size integer encoding (not varint)
///   - Little-endian byte order
///   - No trailing bytes rejected
///   - 2 GiB size limit (generous, prevents OOM on corrupt files)
///
/// Using explicit options guarantees wire-format stability regardless of
/// bincode crate default changes or future upgrades.
fn bincode_options() -> impl bincode::Options {
    bincode::options()
        .with_fixint_encoding()
        .with_little_endian()
        .allow_trailing_bytes()
        .with_limit(2 * 1024 * 1024 * 1024) // 2 GiB
}

/// Magic bytes for the v3 columnar format: "RGF\x03"
const V3_MAGIC: [u8; 4] = [0x52, 0x47, 0x46, 0x03];

/// Current core data version. Bump ONLY when NodeData, EdgeData, or Value enum changes.
/// This is independent of metadata — metadata uses JSON and handles changes via serde defaults.
const CURRENT_CORE_DATA_VERSION: u32 = 1;

/// File format version exposed for tests and diagnostics.
#[allow(dead_code)]
pub const CURRENT_FORMAT_VERSION: u32 = 3;

/// Column section metadata for v3 format.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct V3ColumnSection {
    type_name: String,
    compressed_size: u64,
    row_count: u32,
    columns: HashMap<String, String>, // prop_name → type_tag
}

/// Metadata serialized as JSON in v3 files. All fields use `#[serde(default)]`
/// so that adding/removing fields never breaks existing files.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct FileMetadata {
    /// Core data version at save time — must match or be migratable.
    #[serde(default)]
    core_data_version: u32,
    /// Library version string at save time (e.g. "0.6.5").
    #[serde(default)]
    library_version: String,
    /// Optional schema definition.
    #[serde(default)]
    schema_definition: Option<SchemaDefinition>,
    /// Property index keys to rebuild after load.
    #[serde(default)]
    property_index_keys: Vec<IndexKey>,
    /// Composite index keys to rebuild after load.
    #[serde(default)]
    composite_index_keys: Vec<CompositeIndexKey>,
    /// Range index keys to rebuild after load.
    #[serde(default)]
    range_index_keys: Vec<IndexKey>,
    /// Node type metadata: node_type → { property_name → type_string }
    #[serde(default)]
    node_type_metadata: HashMap<String, HashMap<String, String>>,
    /// Connection type metadata: connection_type → ConnectionTypeInfo
    #[serde(default)]
    connection_type_metadata: HashMap<String, ConnectionTypeInfo>,
    /// Original ID field name per node type (for alias resolution)
    #[serde(default)]
    id_field_aliases: HashMap<String, String>,
    /// Original title field name per node type (for alias resolution)
    #[serde(default)]
    title_field_aliases: HashMap<String, String>,
    /// Auto-vacuum threshold (None = disabled, default Some(0.3))
    #[serde(default = "default_auto_vacuum_threshold")]
    auto_vacuum_threshold: Option<f64>,
    /// Spatial configuration per node type.
    #[serde(default)]
    spatial_configs: HashMap<String, SpatialConfig>,
    /// Timeseries configuration per node type.
    #[serde(default)]
    timeseries_configs: HashMap<String, TimeseriesConfig>,
    /// Temporal configuration per node type (valid_from/valid_to on nodes).
    #[serde(default)]
    temporal_node_configs: HashMap<String, TemporalConfig>,
    /// Temporal configuration per connection type (valid_from/valid_to on edges).
    #[serde(default)]
    temporal_edge_configs: HashMap<String, Vec<TemporalConfig>>,
    /// Timeseries data version: 1 = Vec<Vec<i64>> keys (legacy), 2 = NaiveDate keys.
    #[serde(default = "default_ts_data_version")]
    timeseries_data_version: u32,
    /// v3: compressed size of topology section.
    #[serde(default)]
    topology_compressed_size: u64,
    /// v3: column sections metadata (one per node type).
    #[serde(default)]
    column_sections: Vec<V3ColumnSection>,
    /// v3: compressed size of embedding section (0 if none).
    #[serde(default)]
    embeddings_compressed_size: u64,
    /// v3: compressed size of timeseries section (0 if none).
    #[serde(default)]
    timeseries_compressed_size: u64,
}

fn default_auto_vacuum_threshold() -> Option<f64> {
    Some(0.3)
}

fn default_ts_data_version() -> u32 {
    2
}

// ─── Metadata transfer helpers ───────────────────────────────────────────────

impl FileMetadata {
    /// Build metadata from a DirGraph, leaving v3 section sizes at zero
    /// (caller fills them in after compression).
    fn from_graph(graph: &DirGraph) -> Self {
        FileMetadata {
            core_data_version: CURRENT_CORE_DATA_VERSION,
            library_version: env!("CARGO_PKG_VERSION").to_string(),
            schema_definition: graph.schema_definition.clone(),
            property_index_keys: graph.property_index_keys.clone(),
            composite_index_keys: graph.composite_index_keys.clone(),
            range_index_keys: graph.range_index_keys.clone(),
            node_type_metadata: graph.node_type_metadata.clone(),
            connection_type_metadata: graph.connection_type_metadata.clone(),
            id_field_aliases: graph.id_field_aliases.clone(),
            title_field_aliases: graph.title_field_aliases.clone(),
            auto_vacuum_threshold: graph.auto_vacuum_threshold,
            spatial_configs: graph.spatial_configs.clone(),
            timeseries_configs: graph.timeseries_configs.clone(),
            temporal_node_configs: graph.temporal_node_configs.clone(),
            temporal_edge_configs: graph.temporal_edge_configs.clone(),
            timeseries_data_version: 2,
            // Section sizes filled in by caller:
            topology_compressed_size: 0,
            column_sections: Vec::new(),
            embeddings_compressed_size: 0,
            timeseries_compressed_size: 0,
        }
    }

    /// Apply metadata fields to a DirGraph during load.
    fn apply_to(self, graph: &mut DirGraph) {
        graph.schema_definition = self.schema_definition;
        graph.property_index_keys = self.property_index_keys;
        graph.composite_index_keys = self.composite_index_keys;
        graph.range_index_keys = self.range_index_keys;
        graph.node_type_metadata = self.node_type_metadata;
        graph.connection_type_metadata = self.connection_type_metadata;
        graph.id_field_aliases = self.id_field_aliases;
        graph.title_field_aliases = self.title_field_aliases;
        graph.auto_vacuum_threshold = self.auto_vacuum_threshold;
        graph.spatial_configs = self.spatial_configs;
        graph.timeseries_configs = self.timeseries_configs;
        graph.temporal_node_configs = self.temporal_node_configs;
        graph.temporal_edge_configs = self.temporal_edge_configs;
        graph.save_metadata = SaveMetadata {
            format_version: 3,
            library_version: self.library_version,
        };
    }
}

// ─── Save ────────────────────────────────────────────────────────────────────

/// Stamp save metadata and snapshot index keys. Quick, runs with GIL held.
pub fn prepare_save(graph: &mut Arc<DirGraph>) {
    let g = Arc::make_mut(graph);
    g.save_metadata = SaveMetadata::current();
    g.populate_index_keys();
}

/// Compress data using zstd (level 1 — fastest with good ratio).
fn zstd_compress(data: &[u8]) -> io::Result<Vec<u8>> {
    zstd::encode_all(std::io::Cursor::new(data), 1)
}

/// Decompress zstd-compressed data.
fn zstd_decompress(data: &[u8]) -> io::Result<Vec<u8>> {
    zstd::decode_all(std::io::Cursor::new(data))
}

/// Serialize a value using the project's pinned bincode options.
fn bincode_ser<T: Serialize>(val: &T) -> io::Result<Vec<u8>> {
    bincode_options().serialize(val).map_err(io::Error::other)
}

/// Deserialize a value using the project's pinned bincode options.
fn bincode_deser<'a, T: Deserialize<'a>>(buf: &'a [u8]) -> io::Result<T> {
    bincode_options()
        .deserialize(buf)
        .map_err(|e| io::Error::other(format!("bincode deserialization failed: {}", e)))
}

/// Serialize, compress, and write graph data to v3 file. Heavy I/O, safe to run without GIL.
///
/// The graph MUST have columnar storage enabled before calling this function.
/// The caller (Python `save()`) handles auto-enable/disable.
pub fn write_graph_v3(graph: &DirGraph, path: &str) -> io::Result<()> {
    // 1. Serialize topology with properties stripped (v3: node props are in column sections)
    let topology_raw = {
        let _strip = StripPropertiesGuard::new();
        let _guard = SerdeSerializeGuard::new(&graph.interner);
        bincode_ser(&graph.graph)?
    };
    let topology_compressed = zstd_compress(&topology_raw)?;
    drop(topology_raw); // free before compressing columns

    // 2. Serialize column sections (one per node type)
    let mut column_sections_meta: Vec<V3ColumnSection> = Vec::new();
    let mut column_sections_data: Vec<Vec<u8>> = Vec::new();

    for (type_name, store) in &graph.column_stores {
        let packed = store.write_packed(&graph.interner)?;
        let compressed = zstd_compress(&packed)?;
        drop(packed); // free uncompressed before next type

        // Build column schema
        let mut cols = HashMap::new();
        for (slot, ik) in store.schema().iter() {
            let prop_name = graph.interner.resolve(ik);
            if let Some(col) = store.columns_ref().get(slot as usize) {
                cols.insert(prop_name.to_string(), col.type_tag().to_string());
            }
        }

        column_sections_meta.push(V3ColumnSection {
            type_name: type_name.clone(),
            compressed_size: compressed.len() as u64,
            row_count: store.row_count(),
            columns: cols,
        });
        column_sections_data.push(compressed);
    }

    // 3. Compress embeddings if any
    let embedding_compressed = if !graph.embeddings.is_empty() {
        let raw = bincode_ser(&graph.embeddings)?;
        Some(zstd_compress(&raw)?)
    } else {
        None
    };

    // 4. Compress timeseries if any
    let timeseries_compressed = if !graph.timeseries_store.is_empty() {
        let raw = bincode_ser(&graph.timeseries_store)?;
        Some(zstd_compress(&raw)?)
    } else {
        None
    };

    // 5. Build metadata (common fields from graph, then fill in section sizes)
    let mut metadata = FileMetadata::from_graph(graph);
    metadata.topology_compressed_size = topology_compressed.len() as u64;
    metadata.column_sections = column_sections_meta;
    metadata.embeddings_compressed_size = embedding_compressed
        .as_ref()
        .map(|b| b.len() as u64)
        .unwrap_or(0);
    metadata.timeseries_compressed_size = timeseries_compressed
        .as_ref()
        .map(|b| b.len() as u64)
        .unwrap_or(0);

    let metadata_json = serde_json::to_vec(&metadata).map_err(io::Error::other)?;

    // 6. Write file
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);

    // Header: magic (4B) + core_data_version (4B) + metadata_length (4B)
    writer.write_all(&V3_MAGIC)?;
    writer.write_all(&CURRENT_CORE_DATA_VERSION.to_le_bytes())?;
    writer.write_all(&(metadata_json.len() as u32).to_le_bytes())?;
    writer.write_all(&metadata_json)?;

    // Topology section
    writer.write_all(&topology_compressed)?;

    // Column sections (one per node type, in metadata order)
    for section_data in &column_sections_data {
        writer.write_all(section_data)?;
    }

    // Embeddings section
    if let Some(emb_data) = &embedding_compressed {
        writer.write_all(emb_data)?;
    }

    // Timeseries section
    if let Some(ts_data) = &timeseries_compressed {
        writer.write_all(ts_data)?;
    }

    writer.flush()?;
    Ok(())
}

// ─── Load ────────────────────────────────────────────────────────────────────

/// Minimum file size to use mmap for the initial file read.
/// Below this threshold, `std::fs::read()` is faster (avoids mmap syscall overhead).
const FILE_MMAP_THRESHOLD: u64 = 65_536; // 64 KB

pub fn load_file(path: &str) -> io::Result<KnowledgeGraph> {
    let file = File::open(path)?;
    let file_len = file.metadata()?.len();

    // For large files, mmap avoids the full copy into a Vec<u8>
    if file_len >= FILE_MMAP_THRESHOLD {
        let mmap = unsafe { Mmap::map(&file)? };
        if mmap.len() < 4 {
            return Err(io::Error::other(
                "File is too small to be a valid kglite file.",
            ));
        }
        if mmap[..4] == V3_MAGIC {
            return load_v3(&mmap);
        }
        return Err(io::Error::other(
            "Unrecognized file format. This file was saved with an older version of kglite. \
             Please rebuild the graph with the current version and save again.",
        ));
    }

    // Small files: direct read is faster
    let buf = std::fs::read(path)?;
    if buf.len() < 4 {
        return Err(io::Error::other(
            "File is too small to be a valid kglite file.",
        ));
    }
    if buf[..4] == V3_MAGIC {
        load_v3(&buf)
    } else {
        Err(io::Error::other(
            "Unrecognized file format. This file was saved with an older version of kglite. \
             Please rebuild the graph with the current version and save again.",
        ))
    }
}

/// Load v3 columnar format.
fn load_v3(buf: &[u8]) -> io::Result<KnowledgeGraph> {
    if buf.len() < 12 {
        return Err(io::Error::other(
            "v3 file is truncated — header incomplete.",
        ));
    }

    // Parse header
    let core_version = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
    let metadata_len = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]) as usize;

    if core_version > CURRENT_CORE_DATA_VERSION {
        return Err(io::Error::other(format!(
            "File uses core data version {} but this library only supports up to version {}. \
             Please upgrade kglite.",
            core_version, CURRENT_CORE_DATA_VERSION,
        )));
    }

    let metadata_end = 12 + metadata_len;
    if buf.len() < metadata_end {
        return Err(io::Error::other(
            "v3 file is truncated — metadata incomplete.",
        ));
    }

    // Parse JSON metadata
    let metadata: FileMetadata = serde_json::from_slice(&buf[12..metadata_end])
        .map_err(|e| io::Error::other(format!("Failed to parse v3 metadata: {}", e)))?;

    // Section offsets
    let topology_start = metadata_end;
    let topology_end = topology_start + metadata.topology_compressed_size as usize;

    // Check bounds before accessing topology section
    if buf.len() < topology_end {
        return Err(io::Error::other(
            "v3 file is truncated — topology section incomplete.",
        ));
    }

    // Decompress + deserialize topology (properties are empty maps)
    let topology_compressed = &buf[topology_start..topology_end];
    let topology_raw = zstd_decompress(topology_compressed)?;

    let mut interner = StringInterner::new();
    let graph: crate::graph::schema::Graph = {
        let _guard = SerdeDeserializeGuard::new(&mut interner);
        bincode_deser(&topology_raw)?
    };
    drop(topology_raw);

    // Extract v3 section metadata before apply_to consumes the rest
    let column_sections = metadata.column_sections.clone();
    let embeddings_compressed_size = metadata.embeddings_compressed_size;
    let timeseries_compressed_size = metadata.timeseries_compressed_size;

    // Reassemble DirGraph
    let mut dir_graph = DirGraph::from_graph(graph);
    dir_graph.interner = interner;
    metadata.apply_to(&mut dir_graph);

    // Rebuild type indices and schemas (needed for ColumnStore construction).
    // Note: rebuild_indices_from_keys is deferred until after column loading
    // because properties are empty at this point (stripped during save).
    dir_graph.rebuild_type_indices_and_compact();
    dir_graph.build_connection_types_cache();

    // Load column sections one type at a time
    let mut section_offset = topology_end;

    // Create temp directory for mmap column files (unique per load to avoid collisions)
    let temp_dir = std::env::temp_dir().join(format!(
        "kglite_v3_{}_{:x}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    // Register for cleanup on DirGraph drop
    if let Ok(mut dirs) = dir_graph.temp_dirs.lock() {
        dirs.push(temp_dir.clone());
    }

    for section_meta in &column_sections {
        let section_end = section_offset + section_meta.compressed_size as usize;
        if buf.len() < section_end {
            return Err(io::Error::other(format!(
                "v3 file truncated — column section '{}' incomplete.",
                section_meta.type_name
            )));
        }

        let compressed = &buf[section_offset..section_end];
        let packed = zstd_decompress(compressed)?;

        // Get the type schema
        if let Some(type_schema) = dir_graph.type_schemas.get(&section_meta.type_name) {
            let type_meta = dir_graph
                .node_type_metadata
                .get(&section_meta.type_name)
                .cloned()
                .unwrap_or_default();

            // Create temp dir for this type's column files
            let type_temp_dir = temp_dir.join(&section_meta.type_name);
            std::fs::create_dir_all(&type_temp_dir)?;

            let store = ColumnStore::load_packed(
                Arc::clone(type_schema),
                &type_meta,
                &dir_graph.interner,
                &packed,
                section_meta.row_count,
                Some(&type_temp_dir),
            )?;
            drop(packed); // free before next type

            dir_graph
                .column_stores
                .insert(section_meta.type_name.clone(), Arc::new(store));
        }

        section_offset = section_end;
    }

    // Re-point nodes to columnar storage
    for (type_name, store) in &dir_graph.column_stores {
        if let Some(indices) = dir_graph.type_indices.get(type_name) {
            for (row_id, &node_idx) in indices.iter().enumerate() {
                if let Some(node) = dir_graph.graph.node_weight_mut(node_idx) {
                    node.properties = PropertyStorage::Columnar {
                        store: Arc::clone(store),
                        row_id: row_id as u32,
                    };
                }
            }
        }
    }

    // Now that nodes have columnar properties, rebuild property/range/composite indices
    dir_graph.rebuild_indices_from_keys();

    // Load embeddings if present
    if embeddings_compressed_size > 0 {
        let emb_end = section_offset + embeddings_compressed_size as usize;
        if buf.len() >= emb_end {
            let emb_compressed = &buf[section_offset..emb_end];
            let emb_raw = zstd_decompress(emb_compressed)?;
            let embeddings: HashMap<(String, String), EmbeddingStore> = bincode_deser(&emb_raw)?;
            dir_graph.embeddings = embeddings;
            section_offset = emb_end;
        }
    }

    // Load timeseries if present
    if timeseries_compressed_size > 0 {
        let ts_end = section_offset + timeseries_compressed_size as usize;
        if buf.len() >= ts_end {
            let ts_compressed = &buf[section_offset..ts_end];
            let ts_raw = zstd_decompress(ts_compressed)?;
            let ts_store: HashMap<usize, NodeTimeseries> = bincode_deser(&ts_raw)?;
            dir_graph.timeseries_store = ts_store;
        }
    }

    Ok(KnowledgeGraph {
        inner: Arc::new(dir_graph),
        selection: CowSelection::new(),
        reports: OperationReports::new(),
        last_mutation_stats: None,
        #[cfg(feature = "python")]
        embedder: None,
        temporal_context: TemporalContext::default(),
    })
}

// ─── Embedding Export / Import ────────────────────────────────────────────

use crate::datatypes::values::Value;

/// Magic bytes for the embedding export format.
const KGLE_MAGIC: [u8; 4] = *b"KGLE";
const KGLE_VERSION: u32 = 1;

/// A single embedding store serialized with node IDs (not internal indices).
#[derive(Serialize, Deserialize)]
struct ExportedEmbeddingStore {
    node_type: String,
    text_column: String, // e.g. "summary" (without _emb suffix)
    dimension: usize,
    entries: Vec<(Value, Vec<f32>)>, // (node_id, embedding) pairs
}

/// Filter for selective embedding export.
pub enum EmbeddingExportFilter {
    /// Export all embedding stores for these node types.
    Types(Vec<String>),
    /// Export specific (node_type → [text_columns]) pairs.
    /// An empty vec means all properties for that type.
    TypeProperties(HashMap<String, Vec<String>>),
}

pub struct ExportStats {
    pub stores: usize,
    pub embeddings: usize,
}

pub struct ImportStats {
    pub stores: usize,
    pub imported: usize,
    pub skipped: usize,
}

/// Export embeddings to a standalone .kgle file, keyed by node ID.
pub fn export_embeddings_to_file(
    graph: &DirGraph,
    path: &str,
    filter: Option<&EmbeddingExportFilter>,
) -> io::Result<ExportStats> {
    let mut exported_stores: Vec<ExportedEmbeddingStore> = Vec::new();
    let mut total_embeddings = 0usize;

    for ((node_type, store_name), store) in &graph.embeddings {
        let text_column = store_name
            .strip_suffix("_emb")
            .unwrap_or(store_name.as_str());

        // Apply filter
        if let Some(f) = filter {
            match f {
                EmbeddingExportFilter::Types(types) => {
                    if !types.iter().any(|t| t == node_type) {
                        continue;
                    }
                }
                EmbeddingExportFilter::TypeProperties(map) => {
                    match map.get(node_type) {
                        None => continue, // type not in filter
                        Some(props) if !props.is_empty() => {
                            if !props.iter().any(|p| p == text_column) {
                                continue;
                            }
                        }
                        Some(_) => {} // empty list = all properties for this type
                    }
                }
            }
        }

        // Resolve node indices → node IDs
        let mut entries: Vec<(Value, Vec<f32>)> = Vec::with_capacity(store.len());
        for &node_index in &store.slot_to_node {
            if let Some(node) = graph
                .graph
                .node_weight(petgraph::graph::NodeIndex::new(node_index))
            {
                if let Some(embedding) = store.get_embedding(node_index) {
                    entries.push((node.id.clone(), embedding.to_vec()));
                }
            }
        }

        total_embeddings += entries.len();
        exported_stores.push(ExportedEmbeddingStore {
            node_type: node_type.clone(),
            text_column: text_column.to_string(),
            dimension: store.dimension,
            entries,
        });
    }

    // Write: magic + version + gzip(bincode(stores))
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    writer.write_all(&KGLE_MAGIC)?;
    writer.write_all(&KGLE_VERSION.to_le_bytes())?;

    let gz = GzEncoder::new(&mut writer, Compression::new(3));
    bincode_options()
        .serialize_into(gz, &exported_stores)
        .map_err(|e| io::Error::other(format!("Failed to serialize embeddings: {}", e)))?;

    writer.flush()?;

    Ok(ExportStats {
        stores: exported_stores.len(),
        embeddings: total_embeddings,
    })
}

/// Import embeddings from a .kgle file, resolving node IDs to current graph indices.
pub fn import_embeddings_from_file(graph: &mut DirGraph, path: &str) -> io::Result<ImportStats> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf)?;

    if buf.len() < 8 {
        return Err(io::Error::other(
            "File is too small to be a valid .kgle file.",
        ));
    }

    // Validate magic and version
    if buf[..4] != KGLE_MAGIC {
        return Err(io::Error::other(
            "Not a valid .kgle file (bad magic bytes).",
        ));
    }
    let version = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
    if version > KGLE_VERSION {
        return Err(io::Error::other(format!(
            "Embedding file version {} is newer than supported version {}. Please upgrade kglite.",
            version, KGLE_VERSION,
        )));
    }

    // Decompress and deserialize
    let gz = GzDecoder::new(&buf[8..]);
    let exported_stores: Vec<ExportedEmbeddingStore> = bincode_options()
        .deserialize_from(gz)
        .map_err(|e| io::Error::other(format!("Failed to deserialize embedding data: {}", e)))?;

    let mut total_imported = 0usize;
    let mut total_skipped = 0usize;
    let mut stores_count = 0usize;

    for exported in exported_stores {
        // Build ID index for this node type so lookup_by_id works
        graph.build_id_index(&exported.node_type);

        let mut store = crate::graph::schema::EmbeddingStore::new(exported.dimension);
        store
            .data
            .reserve(exported.entries.len() * exported.dimension);

        let mut imported = 0usize;
        let mut skipped = 0usize;

        for (id, vec) in &exported.entries {
            match graph.lookup_by_id(&exported.node_type, id) {
                Some(node_idx) => {
                    store.set_embedding(node_idx.index(), vec);
                    imported += 1;
                }
                None => {
                    skipped += 1;
                }
            }
        }

        if imported > 0 {
            let key = (exported.node_type, format!("{}_emb", exported.text_column));
            graph.embeddings.insert(key, store);
            stores_count += 1;
        }

        total_imported += imported;
        total_skipped += skipped;
    }

    Ok(ImportStats {
        stores: stores_count,
        imported: total_imported,
        skipped: total_skipped,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes::values::Value;
    use crate::graph::schema::{DirGraph, EmbeddingStore, NodeData};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    /// Helper: extract the error string from a Result, panicking if Ok.
    fn expect_err_msg<T>(result: io::Result<T>) -> String {
        match result {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected an error but got Ok"),
        }
    }

    /// Helper: unwrap an Arc with a single strong reference.
    fn unwrap_arc(arc: Arc<DirGraph>) -> DirGraph {
        match Arc::try_unwrap(arc) {
            Ok(g) => g,
            Err(_) => panic!("Arc has multiple strong references"),
        }
    }

    /// Helper: create a DirGraph with a few nodes of a given type.
    fn make_test_graph() -> DirGraph {
        let mut g = DirGraph::new();

        // Add nodes manually via petgraph
        let mut props1 = HashMap::new();
        props1.insert("name".to_string(), Value::String("Alice".into()));
        props1.insert("age".to_string(), Value::Int64(30));
        let node1 = NodeData::new(
            Value::Int64(1),
            Value::String("Alice".into()),
            "Person".to_string(),
            props1,
            &mut g.interner,
        );
        let idx1 = g.graph.add_node(node1);

        let mut props2 = HashMap::new();
        props2.insert("name".to_string(), Value::String("Bob".into()));
        props2.insert("age".to_string(), Value::Int64(25));
        let node2 = NodeData::new(
            Value::Int64(2),
            Value::String("Bob".into()),
            "Person".to_string(),
            props2,
            &mut g.interner,
        );
        let idx2 = g.graph.add_node(node2);

        // Add an edge
        let conn_key = g.interner.get_or_intern("KNOWS");
        g.graph.add_edge(
            idx1,
            idx2,
            crate::graph::schema::EdgeData {
                connection_type: conn_key,
                properties: Vec::new(),
            },
        );

        // Build type metadata (needed for columnar)
        let mut person_meta = HashMap::new();
        person_meta.insert("name".to_string(), "String".to_string());
        person_meta.insert("age".to_string(), "Int64".to_string());
        g.node_type_metadata
            .insert("Person".to_string(), person_meta);

        // Rebuild indices
        g.rebuild_type_indices_and_compact();
        g.build_connection_types_cache();

        g
    }

    // ========================================================================
    // zstd compression roundtrip
    // ========================================================================

    #[test]
    fn test_zstd_roundtrip_empty() {
        let data = b"";
        let compressed = zstd_compress(data).unwrap();
        let decompressed = zstd_decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zstd_roundtrip_small() {
        let data = b"hello world this is a test of zstd compression";
        let compressed = zstd_compress(data).unwrap();
        let decompressed = zstd_decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zstd_roundtrip_large() {
        let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        let compressed = zstd_compress(&data).unwrap();
        assert!(
            compressed.len() < data.len(),
            "compression should reduce size"
        );
        let decompressed = zstd_decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zstd_decompress_invalid() {
        let garbage = b"this is not valid zstd data";
        assert!(zstd_decompress(garbage).is_err());
    }

    // ========================================================================
    // bincode serialization roundtrip
    // ========================================================================

    #[test]
    fn test_bincode_roundtrip_simple_types() {
        let val: i64 = 42;
        let bytes = bincode_ser(&val).unwrap();
        let restored: i64 = bincode_deser(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn test_bincode_roundtrip_string() {
        let val = "hello world".to_string();
        let bytes = bincode_ser(&val).unwrap();
        let restored: String = bincode_deser(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn test_bincode_roundtrip_hashmap() {
        let mut map: HashMap<String, i32> = HashMap::new();
        map.insert("a".to_string(), 1);
        map.insert("b".to_string(), 2);
        let bytes = bincode_ser(&map).unwrap();
        let restored: HashMap<String, i32> = bincode_deser(&bytes).unwrap();
        assert_eq!(map, restored);
    }

    #[test]
    fn test_bincode_roundtrip_vec() {
        let val = vec![1u8, 2, 3, 4, 5];
        let bytes = bincode_ser(&val).unwrap();
        let restored: Vec<u8> = bincode_deser(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn test_bincode_deser_invalid() {
        // empty buffer cannot deserialize an i64
        let result: io::Result<i64> = bincode_deser(&[]);
        assert!(result.is_err());
    }

    // ========================================================================
    // Default helpers
    // ========================================================================

    #[test]
    fn test_default_auto_vacuum_threshold() {
        assert_eq!(default_auto_vacuum_threshold(), Some(0.3));
    }

    #[test]
    fn test_default_ts_data_version() {
        assert_eq!(default_ts_data_version(), 2);
    }

    // ========================================================================
    // FileMetadata
    // ========================================================================

    #[test]
    fn test_file_metadata_from_graph_basic() {
        let g = make_test_graph();
        let meta = FileMetadata::from_graph(&g);

        assert_eq!(meta.core_data_version, CURRENT_CORE_DATA_VERSION);
        assert!(!meta.library_version.is_empty());
        assert_eq!(meta.auto_vacuum_threshold, Some(0.3));
        assert_eq!(meta.timeseries_data_version, 2);
        // Section sizes should be zero (caller fills in)
        assert_eq!(meta.topology_compressed_size, 0);
        assert!(meta.column_sections.is_empty());
        assert_eq!(meta.embeddings_compressed_size, 0);
        assert_eq!(meta.timeseries_compressed_size, 0);
    }

    #[test]
    fn test_file_metadata_preserves_node_type_metadata() {
        let g = make_test_graph();
        let meta = FileMetadata::from_graph(&g);
        assert!(meta.node_type_metadata.contains_key("Person"));
        let person = &meta.node_type_metadata["Person"];
        assert_eq!(person.get("name").unwrap(), "String");
        assert_eq!(person.get("age").unwrap(), "Int64");
    }

    #[test]
    fn test_file_metadata_preserves_id_field_aliases() {
        let mut g = make_test_graph();
        g.id_field_aliases
            .insert("Person".to_string(), "npdid".to_string());
        g.title_field_aliases
            .insert("Person".to_string(), "prospect_name".to_string());

        let meta = FileMetadata::from_graph(&g);
        assert_eq!(meta.id_field_aliases.get("Person").unwrap(), "npdid");
        assert_eq!(
            meta.title_field_aliases.get("Person").unwrap(),
            "prospect_name"
        );
    }

    #[test]
    fn test_file_metadata_apply_to() {
        let g = make_test_graph();
        let meta = FileMetadata::from_graph(&g);
        let lib_version = meta.library_version.clone();
        let node_meta = meta.node_type_metadata.clone();

        let mut new_graph = DirGraph::new();
        meta.apply_to(&mut new_graph);

        assert_eq!(new_graph.save_metadata.format_version, 3);
        assert_eq!(new_graph.save_metadata.library_version, lib_version);
        assert_eq!(new_graph.node_type_metadata, node_meta);
        assert_eq!(new_graph.auto_vacuum_threshold, Some(0.3));
    }

    #[test]
    fn test_file_metadata_json_roundtrip() {
        let g = make_test_graph();
        let meta = FileMetadata::from_graph(&g);

        let json = serde_json::to_vec(&meta).unwrap();
        let restored: FileMetadata = serde_json::from_slice(&json).unwrap();

        assert_eq!(restored.core_data_version, meta.core_data_version);
        assert_eq!(restored.library_version, meta.library_version);
        assert_eq!(
            restored.node_type_metadata.len(),
            meta.node_type_metadata.len()
        );
        assert_eq!(restored.auto_vacuum_threshold, meta.auto_vacuum_threshold);
    }

    #[test]
    fn test_file_metadata_json_with_unknown_fields() {
        // Simulate loading metadata from a newer version with extra fields
        let json = r#"{
            "core_data_version": 1,
            "library_version": "99.0.0",
            "future_field": "should be ignored",
            "timeseries_data_version": 2
        }"#;
        let meta: FileMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(meta.core_data_version, 1);
        assert_eq!(meta.library_version, "99.0.0");
        // defaults should be applied for missing fields
        assert_eq!(meta.auto_vacuum_threshold, Some(0.3));
        assert!(meta.node_type_metadata.is_empty());
    }

    #[test]
    fn test_file_metadata_json_empty_object() {
        // All defaults should apply
        let meta: FileMetadata = serde_json::from_str("{}").unwrap();
        assert_eq!(meta.core_data_version, 0);
        assert!(meta.library_version.is_empty());
        assert_eq!(meta.auto_vacuum_threshold, Some(0.3));
        assert_eq!(meta.timeseries_data_version, 2);
    }

    // ========================================================================
    // V3ColumnSection serde
    // ========================================================================

    #[test]
    fn test_v3_column_section_roundtrip() {
        let mut cols = HashMap::new();
        cols.insert("name".to_string(), "String".to_string());
        cols.insert("age".to_string(), "Int64".to_string());

        let section = V3ColumnSection {
            type_name: "Person".to_string(),
            compressed_size: 1234,
            row_count: 42,
            columns: cols,
        };

        let json = serde_json::to_string(&section).unwrap();
        let restored: V3ColumnSection = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.type_name, "Person");
        assert_eq!(restored.compressed_size, 1234);
        assert_eq!(restored.row_count, 42);
        assert_eq!(restored.columns.len(), 2);
    }

    // ========================================================================
    // prepare_save
    // ========================================================================

    #[test]
    fn test_prepare_save() {
        let g = make_test_graph();
        let mut arc = Arc::new(g);
        prepare_save(&mut arc);

        assert_eq!(arc.save_metadata.format_version, 3);
        assert!(!arc.save_metadata.library_version.is_empty());
    }

    // ========================================================================
    // Constants
    // ========================================================================

    #[test]
    fn test_v3_magic() {
        assert_eq!(&V3_MAGIC, b"RGF\x03");
    }

    #[test]
    fn test_current_format_version() {
        assert_eq!(CURRENT_FORMAT_VERSION, 3);
    }

    #[test]
    fn test_kgle_magic() {
        assert_eq!(&KGLE_MAGIC, b"KGLE");
    }

    // ========================================================================
    // load_file error paths
    // ========================================================================

    #[test]
    fn test_load_file_nonexistent() {
        let result = load_file("/tmp/does_not_exist_kglite_test.kgl");
        assert!(result.is_err());
    }

    #[test]
    fn test_load_file_too_small() {
        let tmp = NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), b"AB").unwrap();
        let err_msg = expect_err_msg(load_file(tmp.path().to_str().unwrap()));
        assert!(
            err_msg.contains("too small"),
            "unexpected error: {}",
            err_msg
        );
    }

    #[test]
    fn test_load_file_bad_magic() {
        let tmp = NamedTempFile::new().unwrap();
        // 4 bytes that are not a recognized magic
        std::fs::write(tmp.path(), b"XXXX_extra_data_here").unwrap();
        let err_msg = expect_err_msg(load_file(tmp.path().to_str().unwrap()));
        assert!(
            err_msg.contains("Unrecognized file format"),
            "unexpected error: {}",
            err_msg
        );
    }

    #[test]
    fn test_load_file_empty() {
        let tmp = NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), b"").unwrap();
        let err_msg = expect_err_msg(load_file(tmp.path().to_str().unwrap()));
        assert!(
            err_msg.contains("too small"),
            "unexpected error: {}",
            err_msg
        );
    }

    // ========================================================================
    // load_v3 error paths
    // ========================================================================

    #[test]
    fn test_load_v3_truncated_header() {
        // Valid magic but not enough bytes for header
        let mut buf = V3_MAGIC.to_vec();
        buf.extend_from_slice(&[0u8; 4]); // only 8 bytes total, need 12
        let err_msg = expect_err_msg(load_v3(&buf));
        assert!(
            err_msg.contains("truncated") || err_msg.contains("header incomplete"),
            "unexpected error: {}",
            err_msg
        );
    }

    #[test]
    fn test_load_v3_future_core_version() {
        let mut buf = V3_MAGIC.to_vec();
        // core_data_version = 999
        buf.extend_from_slice(&999u32.to_le_bytes());
        // metadata_length = 0
        buf.extend_from_slice(&0u32.to_le_bytes());
        let err_msg = expect_err_msg(load_v3(&buf));
        assert!(
            err_msg.contains("upgrade kglite"),
            "unexpected error: {}",
            err_msg
        );
    }

    #[test]
    fn test_load_v3_truncated_metadata() {
        let mut buf = V3_MAGIC.to_vec();
        buf.extend_from_slice(&CURRENT_CORE_DATA_VERSION.to_le_bytes());
        // metadata_length = 1000, but we don't provide that many bytes
        buf.extend_from_slice(&1000u32.to_le_bytes());
        buf.extend_from_slice(&[0u8; 10]); // only 10 bytes of metadata
        let err_msg = expect_err_msg(load_v3(&buf));
        assert!(
            err_msg.contains("truncated") || err_msg.contains("metadata incomplete"),
            "unexpected error: {}",
            err_msg
        );
    }

    #[test]
    fn test_load_v3_invalid_json_metadata() {
        let mut buf = V3_MAGIC.to_vec();
        buf.extend_from_slice(&CURRENT_CORE_DATA_VERSION.to_le_bytes());
        let bad_json = b"this is not json{{{";
        buf.extend_from_slice(&(bad_json.len() as u32).to_le_bytes());
        buf.extend_from_slice(bad_json);
        let err_msg = expect_err_msg(load_v3(&buf));
        assert!(
            err_msg.contains("parse") || err_msg.contains("metadata"),
            "unexpected error: {}",
            err_msg
        );
    }

    // ========================================================================
    // v3 write + load roundtrip
    // ========================================================================

    #[test]
    fn test_write_and_load_v3_roundtrip() {
        let mut g = make_test_graph();
        // Enable columnar (required for v3 write)
        g.enable_columnar();

        // Prepare save (stamps metadata, snapshots index keys)
        let mut arc = Arc::new(g);
        prepare_save(&mut arc);
        let g = unwrap_arc(arc);

        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();

        write_graph_v3(&g, path).unwrap();

        // Verify file starts with v3 magic
        let file_bytes = std::fs::read(path).unwrap();
        assert_eq!(&file_bytes[..4], &V3_MAGIC);

        // Load it back
        let kg = load_file(path).unwrap();
        let loaded = &*kg.inner;

        // Check node count
        assert_eq!(loaded.graph.node_count(), 2);
        // Check edge count
        assert_eq!(loaded.graph.edge_count(), 1);

        // Check node type metadata was preserved
        assert!(loaded.node_type_metadata.contains_key("Person"));

        // Check save metadata
        assert_eq!(loaded.save_metadata.format_version, 3);
    }

    #[test]
    fn test_write_and_load_v3_preserves_node_data() {
        let mut g = make_test_graph();
        g.enable_columnar();
        let mut arc = Arc::new(g);
        prepare_save(&mut arc);
        let g = unwrap_arc(arc);

        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();
        write_graph_v3(&g, path).unwrap();

        let kg = load_file(path).unwrap();
        let loaded = &*kg.inner;

        // Find node with id=1 and check title
        let mut found_alice = false;
        for idx in loaded.graph.node_indices() {
            if let Some(node) = loaded.graph.node_weight(idx) {
                if node.id == Value::Int64(1) {
                    assert_eq!(node.title, Value::String("Alice".into()));
                    assert_eq!(node.node_type, "Person");
                    found_alice = true;
                }
            }
        }
        assert!(found_alice, "Alice node not found after roundtrip");
    }

    #[test]
    fn test_write_and_load_v3_with_aliases() {
        let mut g = make_test_graph();
        g.id_field_aliases
            .insert("Person".to_string(), "person_id".to_string());
        g.title_field_aliases
            .insert("Person".to_string(), "full_name".to_string());
        g.enable_columnar();

        let mut arc = Arc::new(g);
        prepare_save(&mut arc);
        let g = unwrap_arc(arc);

        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();
        write_graph_v3(&g, path).unwrap();

        let kg = load_file(path).unwrap();
        let loaded = &*kg.inner;
        assert_eq!(loaded.id_field_aliases.get("Person").unwrap(), "person_id");
        assert_eq!(
            loaded.title_field_aliases.get("Person").unwrap(),
            "full_name"
        );
    }

    #[test]
    fn test_write_and_load_v3_empty_graph() {
        let mut g = DirGraph::new();
        g.rebuild_type_indices_and_compact();
        g.enable_columnar();

        let mut arc = Arc::new(g);
        prepare_save(&mut arc);
        let g = unwrap_arc(arc);

        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();
        write_graph_v3(&g, path).unwrap();

        let kg = load_file(path).unwrap();
        assert_eq!(kg.inner.graph.node_count(), 0);
        assert_eq!(kg.inner.graph.edge_count(), 0);
    }

    #[test]
    fn test_write_and_load_v3_with_embeddings() {
        let mut g = make_test_graph();

        // Add embedding store
        let mut store = EmbeddingStore::new(3);
        // Use node index 0 (first node added)
        store.set_embedding(0, &[1.0, 2.0, 3.0]);
        store.set_embedding(1, &[4.0, 5.0, 6.0]);
        g.embeddings
            .insert(("Person".to_string(), "desc_emb".to_string()), store);

        g.enable_columnar();

        let mut arc = Arc::new(g);
        prepare_save(&mut arc);
        let g = unwrap_arc(arc);

        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();
        write_graph_v3(&g, path).unwrap();

        let kg = load_file(path).unwrap();
        let loaded = &*kg.inner;

        let key = ("Person".to_string(), "desc_emb".to_string());
        assert!(
            loaded.embeddings.contains_key(&key),
            "embedding store not found after roundtrip"
        );
        let loaded_store = &loaded.embeddings[&key];
        assert_eq!(loaded_store.dimension, 3);
        assert_eq!(loaded_store.get_embedding(0).unwrap(), &[1.0, 2.0, 3.0]);
        assert_eq!(loaded_store.get_embedding(1).unwrap(), &[4.0, 5.0, 6.0]);
    }

    // ========================================================================
    // load_file mmap vs direct read path
    // ========================================================================

    #[test]
    fn test_load_file_large_file_uses_mmap_path() {
        // Write a valid v3 file larger than FILE_MMAP_THRESHOLD (64KB)
        let mut g = DirGraph::new();
        // Add enough nodes to produce a file > 64KB. Use unique per-node strings that
        // are long enough that even with compression the file exceeds FILE_MMAP_THRESHOLD.
        for i in 0..2000 {
            let mut props = HashMap::new();
            // Build a unique 500-char string per node to resist compression
            let unique_part: String = (0..50).map(|j| format!("{:010}", i * 50 + j)).collect();
            props.insert("data".to_string(), Value::String(unique_part));
            let node = NodeData::new(
                Value::Int64(i),
                Value::String(format!("Node {}", i)),
                "BigType".to_string(),
                props,
                &mut g.interner,
            );
            g.graph.add_node(node);
        }

        let mut big_meta = HashMap::new();
        big_meta.insert("data".to_string(), "String".to_string());
        g.node_type_metadata.insert("BigType".to_string(), big_meta);

        g.rebuild_type_indices_and_compact();
        g.enable_columnar();

        let mut arc = Arc::new(g);
        prepare_save(&mut arc);
        let g = unwrap_arc(arc);

        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();
        write_graph_v3(&g, path).unwrap();

        // Verify file is large enough to trigger mmap
        let file_size = std::fs::metadata(path).unwrap().len();
        assert!(
            file_size >= FILE_MMAP_THRESHOLD,
            "file too small to test mmap path: {} bytes",
            file_size
        );

        let kg = load_file(path).unwrap();
        assert_eq!(kg.inner.graph.node_count(), 2000);
    }

    #[test]
    fn test_load_file_bad_magic_large_file() {
        // Write a file larger than mmap threshold but with bad magic
        let tmp = NamedTempFile::new().unwrap();
        let data = vec![0x42u8; FILE_MMAP_THRESHOLD as usize + 100];
        std::fs::write(tmp.path(), &data).unwrap();
        let err_msg = expect_err_msg(load_file(tmp.path().to_str().unwrap()));
        assert!(
            err_msg.contains("Unrecognized"),
            "unexpected error: {}",
            err_msg
        );
    }

    #[test]
    fn test_load_file_too_small_large_mmap() {
        // 4 bytes of magic-like data via mmap path — need >= FILE_MMAP_THRESHOLD
        // but only 3 bytes of content after mmap
        // Actually this won't trigger mmap since file_len < threshold.
        // For mmap path: write exactly FILE_MMAP_THRESHOLD bytes but first 4 = v3 magic,
        // then truncated content
        let tmp = NamedTempFile::new().unwrap();
        let mut data = V3_MAGIC.to_vec();
        // Just enough to trigger mmap but not enough for full header (need 12)
        data.resize(FILE_MMAP_THRESHOLD as usize, 0);
        // Only 8 bytes beyond magic (need 8 more for core_version + meta_len but metadata
        // will point to data that doesn't exist)
        std::fs::write(tmp.path(), &data).unwrap();
        let result = load_file(tmp.path().to_str().unwrap());
        // This should fail during v3 parsing (truncated metadata or bad topology)
        assert!(result.is_err());
    }

    // ========================================================================
    // Embedding export/import
    // ========================================================================

    #[test]
    fn test_export_import_embeddings_roundtrip() {
        let mut g = make_test_graph();
        g.rebuild_type_indices();

        // Add embedding store
        let idx0 = g.type_indices["Person"][0];
        let idx1 = g.type_indices["Person"][1];

        let mut store = EmbeddingStore::new(2);
        store.set_embedding(idx0.index(), &[1.0, 2.0]);
        store.set_embedding(idx1.index(), &[3.0, 4.0]);
        g.embeddings
            .insert(("Person".to_string(), "desc_emb".to_string()), store);

        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();

        // Export
        let export_stats = export_embeddings_to_file(&g, path, None).unwrap();
        assert_eq!(export_stats.stores, 1);
        assert_eq!(export_stats.embeddings, 2);

        // Verify file starts with KGLE magic
        let file_bytes = std::fs::read(path).unwrap();
        assert_eq!(&file_bytes[..4], &KGLE_MAGIC);

        // Import into a copy of the graph
        let mut g2 = make_test_graph();
        g2.rebuild_type_indices();
        let import_stats = import_embeddings_from_file(&mut g2, path).unwrap();
        assert_eq!(import_stats.stores, 1);
        assert_eq!(import_stats.imported, 2);
        assert_eq!(import_stats.skipped, 0);
    }

    #[test]
    fn test_export_embeddings_with_type_filter() {
        let mut g = make_test_graph();
        g.rebuild_type_indices();

        let idx0 = g.type_indices["Person"][0];
        let mut store = EmbeddingStore::new(2);
        store.set_embedding(idx0.index(), &[1.0, 2.0]);
        g.embeddings
            .insert(("Person".to_string(), "desc_emb".to_string()), store);

        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();

        // Filter for a type that doesn't exist — should export 0 stores
        let filter = EmbeddingExportFilter::Types(vec!["Animal".to_string()]);
        let stats = export_embeddings_to_file(&g, path, Some(&filter)).unwrap();
        assert_eq!(stats.stores, 0);
        assert_eq!(stats.embeddings, 0);

        // Filter for Person — should export 1 store
        let filter = EmbeddingExportFilter::Types(vec!["Person".to_string()]);
        let stats = export_embeddings_to_file(&g, path, Some(&filter)).unwrap();
        assert_eq!(stats.stores, 1);
        assert_eq!(stats.embeddings, 1);
    }

    #[test]
    fn test_export_embeddings_with_property_filter() {
        let mut g = make_test_graph();
        g.rebuild_type_indices();

        let idx0 = g.type_indices["Person"][0];
        let mut store = EmbeddingStore::new(2);
        store.set_embedding(idx0.index(), &[1.0, 2.0]);
        g.embeddings
            .insert(("Person".to_string(), "desc_emb".to_string()), store);

        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();

        // Filter for specific property that doesn't match
        let mut map = HashMap::new();
        map.insert("Person".to_string(), vec!["summary".to_string()]);
        let filter = EmbeddingExportFilter::TypeProperties(map);
        let stats = export_embeddings_to_file(&g, path, Some(&filter)).unwrap();
        assert_eq!(stats.stores, 0);

        // Filter matching property "desc"
        let mut map = HashMap::new();
        map.insert("Person".to_string(), vec!["desc".to_string()]);
        let filter = EmbeddingExportFilter::TypeProperties(map);
        let stats = export_embeddings_to_file(&g, path, Some(&filter)).unwrap();
        assert_eq!(stats.stores, 1);

        // Empty property list = all properties for that type
        let mut map = HashMap::new();
        map.insert("Person".to_string(), vec![]);
        let filter = EmbeddingExportFilter::TypeProperties(map);
        let stats = export_embeddings_to_file(&g, path, Some(&filter)).unwrap();
        assert_eq!(stats.stores, 1);

        // Type not in filter map
        let mut map = HashMap::new();
        map.insert("Animal".to_string(), vec![]);
        let filter = EmbeddingExportFilter::TypeProperties(map);
        let stats = export_embeddings_to_file(&g, path, Some(&filter)).unwrap();
        assert_eq!(stats.stores, 0);
    }

    #[test]
    fn test_export_embeddings_no_stores() {
        let g = make_test_graph();
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();

        let stats = export_embeddings_to_file(&g, path, None).unwrap();
        assert_eq!(stats.stores, 0);
        assert_eq!(stats.embeddings, 0);
    }

    #[test]
    fn test_import_embeddings_bad_magic() {
        let tmp = NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), b"XXXX_not_kgle_data_here").unwrap();

        let mut g = make_test_graph();
        let err_msg = expect_err_msg(import_embeddings_from_file(
            &mut g,
            tmp.path().to_str().unwrap(),
        ));
        assert!(
            err_msg.contains("bad magic"),
            "unexpected error: {}",
            err_msg
        );
    }

    #[test]
    fn test_import_embeddings_too_small() {
        let tmp = NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), b"KGLE").unwrap(); // only 4 bytes, need 8

        let mut g = make_test_graph();
        let err_msg = expect_err_msg(import_embeddings_from_file(
            &mut g,
            tmp.path().to_str().unwrap(),
        ));
        assert!(
            err_msg.contains("too small"),
            "unexpected error: {}",
            err_msg
        );
    }

    #[test]
    fn test_import_embeddings_future_version() {
        let tmp = NamedTempFile::new().unwrap();
        let mut data = KGLE_MAGIC.to_vec();
        data.extend_from_slice(&999u32.to_le_bytes());
        data.extend_from_slice(&[0u8; 100]); // junk payload
        std::fs::write(tmp.path(), &data).unwrap();

        let mut g = make_test_graph();
        let err_msg = expect_err_msg(import_embeddings_from_file(
            &mut g,
            tmp.path().to_str().unwrap(),
        ));
        assert!(
            err_msg.contains("upgrade kglite"),
            "unexpected error: {}",
            err_msg
        );
    }

    #[test]
    fn test_import_embeddings_with_missing_nodes() {
        // Export from a graph, import into a different graph with fewer nodes
        let mut g = make_test_graph();
        g.rebuild_type_indices();

        let idx0 = g.type_indices["Person"][0];
        let idx1 = g.type_indices["Person"][1];
        let mut store = EmbeddingStore::new(2);
        store.set_embedding(idx0.index(), &[1.0, 2.0]);
        store.set_embedding(idx1.index(), &[3.0, 4.0]);
        g.embeddings
            .insert(("Person".to_string(), "desc_emb".to_string()), store);

        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();
        export_embeddings_to_file(&g, path, None).unwrap();

        // Import into a graph with only one Person node
        let mut g2 = DirGraph::new();
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".into()));
        let node = NodeData::new(
            Value::Int64(1),
            Value::String("Alice".into()),
            "Person".to_string(),
            props,
            &mut g2.interner,
        );
        g2.graph.add_node(node);
        let mut person_meta = HashMap::new();
        person_meta.insert("name".to_string(), "String".to_string());
        g2.node_type_metadata
            .insert("Person".to_string(), person_meta);
        g2.rebuild_type_indices_and_compact();

        let stats = import_embeddings_from_file(&mut g2, path).unwrap();
        assert_eq!(stats.stores, 1);
        assert_eq!(stats.imported, 1); // Alice found
        assert_eq!(stats.skipped, 1); // Bob not found
    }

    // ========================================================================
    // ExportedEmbeddingStore serde
    // ========================================================================

    #[test]
    fn test_exported_embedding_store_bincode_roundtrip() {
        let store = ExportedEmbeddingStore {
            node_type: "Person".to_string(),
            text_column: "summary".to_string(),
            dimension: 3,
            entries: vec![
                (Value::Int64(1), vec![0.1, 0.2, 0.3]),
                (Value::String("abc".into()), vec![0.4, 0.5, 0.6]),
            ],
        };
        let bytes = bincode_ser(&store).unwrap();
        let restored: ExportedEmbeddingStore = bincode_deser(&bytes).unwrap();
        assert_eq!(restored.node_type, "Person");
        assert_eq!(restored.text_column, "summary");
        assert_eq!(restored.dimension, 3);
        assert_eq!(restored.entries.len(), 2);
    }

    // ========================================================================
    // write_graph_v3 file structure validation
    // ========================================================================

    #[test]
    fn test_v3_file_header_structure() {
        let mut g = make_test_graph();
        g.enable_columnar();
        let mut arc = Arc::new(g);
        prepare_save(&mut arc);
        let g = unwrap_arc(arc);

        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();
        write_graph_v3(&g, path).unwrap();

        let bytes = std::fs::read(path).unwrap();
        // Check magic
        assert_eq!(&bytes[..4], &V3_MAGIC);
        // Check core_data_version
        let cdv = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        assert_eq!(cdv, CURRENT_CORE_DATA_VERSION);
        // Check metadata_length is reasonable
        let meta_len = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        assert!(meta_len > 0, "metadata should not be empty");
        assert!(
            (meta_len as usize) < bytes.len(),
            "metadata_length should not exceed file size"
        );

        // Check embedded metadata is valid JSON
        let meta_end = 12 + meta_len as usize;
        let meta: FileMetadata = serde_json::from_slice(&bytes[12..meta_end]).unwrap();
        assert_eq!(meta.core_data_version, CURRENT_CORE_DATA_VERSION);
        assert!(meta.topology_compressed_size > 0);
    }

    // ========================================================================
    // Multiple node types
    // ========================================================================

    #[test]
    fn test_v3_roundtrip_multiple_types() {
        let mut g = DirGraph::new();

        // Add Person nodes
        for i in 0..3 {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String(format!("Person_{}", i)));
            let node = NodeData::new(
                Value::Int64(i),
                Value::String(format!("P{}", i)),
                "Person".to_string(),
                props,
                &mut g.interner,
            );
            g.graph.add_node(node);
        }

        // Add Company nodes
        for i in 0..2 {
            let mut props = HashMap::new();
            props.insert(
                "company_name".to_string(),
                Value::String(format!("Corp_{}", i)),
            );
            let node = NodeData::new(
                Value::Int64(100 + i),
                Value::String(format!("C{}", i)),
                "Company".to_string(),
                props,
                &mut g.interner,
            );
            g.graph.add_node(node);
        }

        let mut person_meta = HashMap::new();
        person_meta.insert("name".to_string(), "String".to_string());
        g.node_type_metadata
            .insert("Person".to_string(), person_meta);
        let mut company_meta = HashMap::new();
        company_meta.insert("company_name".to_string(), "String".to_string());
        g.node_type_metadata
            .insert("Company".to_string(), company_meta);

        g.rebuild_type_indices_and_compact();
        g.enable_columnar();

        let mut arc = Arc::new(g);
        prepare_save(&mut arc);
        let g = unwrap_arc(arc);

        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();
        write_graph_v3(&g, path).unwrap();

        let kg = load_file(path).unwrap();
        assert_eq!(kg.inner.graph.node_count(), 5);
        assert!(kg.inner.node_type_metadata.contains_key("Person"));
        assert!(kg.inner.node_type_metadata.contains_key("Company"));
    }

    // ========================================================================
    // Columnar property retrieval after load
    // ========================================================================

    // ========================================================================
    // write_graph_v3 error paths
    // ========================================================================

    #[test]
    fn test_write_graph_v3_invalid_path() {
        let mut g = make_test_graph();
        g.enable_columnar();
        let mut arc = Arc::new(g);
        prepare_save(&mut arc);
        let g = unwrap_arc(arc);

        let result = write_graph_v3(&g, "/nonexistent_dir_abc123/file.kgl");
        assert!(result.is_err());
    }

    // ========================================================================
    // v3 roundtrip with timeseries data
    // ========================================================================

    #[test]
    fn test_v3_roundtrip_with_timeseries() {
        use crate::graph::timeseries::NodeTimeseries;
        use chrono::NaiveDate;

        let mut g = make_test_graph();

        // Add timeseries data for node index 0
        let mut channels = HashMap::new();
        channels.insert("temperature".to_string(), vec![20.0, 21.5, 22.0]);
        let ts = NodeTimeseries {
            keys: vec![
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
                NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
            ],
            channels,
        };
        g.timeseries_store.insert(0, ts);

        g.enable_columnar();
        let mut arc = Arc::new(g);
        prepare_save(&mut arc);
        let g = unwrap_arc(arc);

        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();
        write_graph_v3(&g, path).unwrap();

        let kg = load_file(path).unwrap();
        let loaded = &*kg.inner;

        assert!(loaded.timeseries_store.contains_key(&0));
        let loaded_ts = &loaded.timeseries_store[&0];
        assert_eq!(loaded_ts.keys.len(), 3);
        assert_eq!(loaded_ts.channels["temperature"], vec![20.0, 21.5, 22.0]);
    }

    // ========================================================================
    // load_v3 truncated section errors
    // ========================================================================

    #[test]
    fn test_load_v3_truncated_topology() {
        // Create a valid header + metadata that claims a large topology,
        // but the actual data is truncated.
        let meta = FileMetadata {
            core_data_version: CURRENT_CORE_DATA_VERSION,
            topology_compressed_size: 99999, // claims huge topology
            ..Default::default()
        };
        let meta_json = serde_json::to_vec(&meta).unwrap();

        let mut buf = V3_MAGIC.to_vec();
        buf.extend_from_slice(&CURRENT_CORE_DATA_VERSION.to_le_bytes());
        buf.extend_from_slice(&(meta_json.len() as u32).to_le_bytes());
        buf.extend_from_slice(&meta_json);
        // Only add a few bytes of "topology" — not enough
        buf.extend_from_slice(&[0u8; 10]);

        let result = load_v3(&buf);
        // Should fail because topology data is truncated (slice will panic or
        // zstd will fail to decompress)
        assert!(result.is_err());
    }

    // ========================================================================
    // bincode_options trailing bytes
    // ========================================================================

    #[test]
    fn test_bincode_allows_trailing_bytes() {
        let val: i64 = 42;
        let mut bytes = bincode_ser(&val).unwrap();
        // Add trailing garbage — should still deserialize OK
        bytes.extend_from_slice(b"trailing garbage");
        let restored: i64 = bincode_deser(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    // ========================================================================
    // FileMetadata edge cases
    // ========================================================================

    #[test]
    fn test_file_metadata_custom_auto_vacuum_threshold() {
        let mut g = make_test_graph();
        g.auto_vacuum_threshold = Some(0.5);
        let meta = FileMetadata::from_graph(&g);
        assert_eq!(meta.auto_vacuum_threshold, Some(0.5));

        let mut new_g = DirGraph::new();
        meta.apply_to(&mut new_g);
        assert_eq!(new_g.auto_vacuum_threshold, Some(0.5));
    }

    #[test]
    fn test_file_metadata_disabled_auto_vacuum() {
        let mut g = make_test_graph();
        g.auto_vacuum_threshold = None;
        let meta = FileMetadata::from_graph(&g);
        assert_eq!(meta.auto_vacuum_threshold, None);

        // JSON roundtrip preserves None
        let json = serde_json::to_vec(&meta).unwrap();
        let restored: FileMetadata = serde_json::from_slice(&json).unwrap();
        assert_eq!(restored.auto_vacuum_threshold, None);
    }

    #[test]
    fn test_file_metadata_with_connection_type_metadata() {
        let mut g = make_test_graph();
        // The connection type cache is built from edges in make_test_graph
        g.build_connection_types_cache();
        let meta = FileMetadata::from_graph(&g);
        // connection_type_metadata should be transferred
        let mut new_g = DirGraph::new();
        let conn_meta_len = meta.connection_type_metadata.len();
        meta.apply_to(&mut new_g);
        assert_eq!(new_g.connection_type_metadata.len(), conn_meta_len);
    }

    #[test]
    fn test_file_metadata_schema_definition_none() {
        let g = make_test_graph();
        let meta = FileMetadata::from_graph(&g);
        assert!(meta.schema_definition.is_none());
    }

    // ========================================================================
    // Embedding import error paths
    // ========================================================================

    #[test]
    fn test_import_embeddings_nonexistent_file() {
        let mut g = make_test_graph();
        let result = import_embeddings_from_file(&mut g, "/tmp/nonexistent_kglite_test_12345.kgle");
        assert!(result.is_err());
    }

    #[test]
    fn test_import_embeddings_corrupt_gzip_data() {
        let tmp = NamedTempFile::new().unwrap();
        let mut data = KGLE_MAGIC.to_vec();
        data.extend_from_slice(&KGLE_VERSION.to_le_bytes());
        // Invalid gzip data after valid header
        data.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00, 0x00]);
        std::fs::write(tmp.path(), &data).unwrap();

        let mut g = make_test_graph();
        let result = import_embeddings_from_file(&mut g, tmp.path().to_str().unwrap());
        assert!(result.is_err());
    }

    // ========================================================================
    // Export to unwritable path
    // ========================================================================

    #[test]
    fn test_export_embeddings_invalid_path() {
        let g = make_test_graph();
        let result = export_embeddings_to_file(&g, "/nonexistent_dir_abc123/out.kgle", None);
        assert!(result.is_err());
    }

    // ========================================================================
    // v3 roundtrip preserves auto_vacuum_threshold
    // ========================================================================

    #[test]
    fn test_v3_roundtrip_preserves_auto_vacuum_threshold() {
        let mut g = make_test_graph();
        g.auto_vacuum_threshold = Some(0.7);
        g.enable_columnar();

        let mut arc = Arc::new(g);
        prepare_save(&mut arc);
        let g = unwrap_arc(arc);

        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();
        write_graph_v3(&g, path).unwrap();

        let kg = load_file(path).unwrap();
        assert_eq!(kg.inner.auto_vacuum_threshold, Some(0.7));
    }

    // ========================================================================
    // v3 roundtrip with edges preserves connection types
    // ========================================================================

    #[test]
    fn test_v3_roundtrip_preserves_edges() {
        let mut g = make_test_graph();
        g.enable_columnar();

        let mut arc = Arc::new(g);
        prepare_save(&mut arc);
        let g = unwrap_arc(arc);

        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();
        write_graph_v3(&g, path).unwrap();

        let kg = load_file(path).unwrap();
        let loaded = &*kg.inner;

        // Should have KNOWS edge
        assert_eq!(loaded.graph.edge_count(), 1);
        let edge = loaded.graph.edge_indices().next().unwrap();
        let edge_data = loaded.graph.edge_weight(edge).unwrap();
        let conn_type = loaded.interner.resolve(edge_data.connection_type);
        assert_eq!(conn_type, "KNOWS");
    }

    // ========================================================================
    // zstd compress then decompress large repetitive data
    // ========================================================================

    #[test]
    fn test_zstd_compression_ratio() {
        // Highly repetitive data should compress well
        let data: Vec<u8> = "abcdefgh".repeat(10_000).into_bytes();
        let compressed = zstd_compress(&data).unwrap();
        let ratio = compressed.len() as f64 / data.len() as f64;
        assert!(
            ratio < 0.1,
            "expected good compression ratio, got {:.2}",
            ratio
        );
        let decompressed = zstd_decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    // ========================================================================
    // Columnar property retrieval after load
    // ========================================================================

    #[test]
    fn test_v3_roundtrip_columnar_properties_accessible() {
        let mut g = make_test_graph();
        g.enable_columnar();
        let mut arc = Arc::new(g);
        prepare_save(&mut arc);
        let g = unwrap_arc(arc);

        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();
        write_graph_v3(&g, path).unwrap();

        let kg = load_file(path).unwrap();
        let loaded = &*kg.inner;

        // Check that column stores were loaded
        assert!(
            loaded.column_stores.contains_key("Person"),
            "column store for Person not found"
        );

        // Verify nodes use Columnar property storage
        for idx in loaded.graph.node_indices() {
            if let Some(node) = loaded.graph.node_weight(idx) {
                if node.node_type == "Person" {
                    match &node.properties {
                        PropertyStorage::Columnar { .. } => {} // expected
                        other => panic!(
                            "Expected Columnar storage, got {:?}",
                            std::mem::discriminant(other)
                        ),
                    }
                }
            }
        }
    }

    #[test]
    fn test_bincode_deser_with_trailing_bytes() {
        // Since bincode_options allows trailing bytes, this should succeed
        let valid_with_trailing = [42u32, 99u32];
        let serialized = bincode_ser(&valid_with_trailing).unwrap();

        // Append extra bytes
        let mut with_extra = serialized.clone();
        with_extra.push(0xFF);

        // Should still deserialize the first u32
        let result: io::Result<u32> = bincode_deser(&with_extra);
        assert!(result.is_ok());
    }

    #[test]
    fn test_bincode_options_consistency() {
        let options1 = bincode_options();
        let options2 = bincode_options();

        // Both should have the same configuration
        let test_val = 42u32;
        let serialized1 = options1.serialize(&test_val).unwrap();
        let serialized2 = options2.serialize(&test_val).unwrap();

        assert_eq!(serialized1, serialized2);
    }

    #[test]
    fn test_bincode_options_fixint_encoding() {
        let options = bincode_options();

        // Fixed-size integers should always use 4 bytes for u32
        let test_val: u32 = 42;
        let serialized = options.serialize(&test_val).unwrap();

        assert_eq!(serialized.len(), 4); // Fixed 4 bytes for u32
    }

    #[test]
    fn test_bincode_options_little_endian() {
        let options = bincode_options();

        let test_val: u32 = 0x12345678;
        let serialized = options.serialize(&test_val).unwrap();

        // Little endian: least significant byte first
        assert_eq!(serialized[0], 0x78);
        assert_eq!(serialized[1], 0x56);
        assert_eq!(serialized[2], 0x34);
        assert_eq!(serialized[3], 0x12);
    }

    #[test]
    fn test_bincode_ser_bool() {
        let original = true;
        let serialized = bincode_ser(&original).unwrap();
        let deserialized: bool = bincode_deser(&serialized).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_bincode_ser_deserialize_symmetry() {
        let original = 12345u64;
        let serialized = bincode_ser(&original).unwrap();
        let deserialized: u64 = bincode_deser(&serialized).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_bincode_ser_hashmap() {
        let mut original = HashMap::new();
        original.insert("key1".to_string(), 100);
        original.insert("key2".to_string(), 200);

        let serialized = bincode_ser(&original).unwrap();
        let deserialized: HashMap<String, i32> = bincode_deser(&serialized).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_bincode_ser_string() {
        let original = "Hello, World!".to_string();
        let serialized = bincode_ser(&original).unwrap();
        let deserialized: String = bincode_deser(&serialized).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_bincode_ser_vec() {
        let original = vec![1, 2, 3, 4, 5];
        let serialized = bincode_ser(&original).unwrap();
        let deserialized: Vec<i32> = bincode_deser(&serialized).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_current_core_data_version() {
        assert_eq!(CURRENT_CORE_DATA_VERSION, 1);
    }

    #[test]
    fn test_embedding_export_filter_type_properties() {
        let mut props = HashMap::new();
        props.insert("Person".to_string(), vec!["name".to_string()]);
        let filter = EmbeddingExportFilter::TypeProperties(props);

        match filter {
            EmbeddingExportFilter::TypeProperties(map) => {
                assert!(map.contains_key("Person"));
            }
            _ => panic!("Expected TypeProperties variant"),
        }
    }

    #[test]
    fn test_embedding_export_filter_types() {
        let filter =
            EmbeddingExportFilter::Types(vec!["Person".to_string(), "Company".to_string()]);

        match filter {
            EmbeddingExportFilter::Types(types) => {
                assert_eq!(types.len(), 2);
                assert!(types.contains(&"Person".to_string()));
            }
            _ => panic!("Expected Types variant"),
        }
    }

    #[test]
    fn test_embedding_strip_suffix() {
        let full_name = "summary_emb";
        let stripped = full_name.strip_suffix("_emb").unwrap_or(full_name);
        assert_eq!(stripped, "summary");
    }

    #[test]
    fn test_embedding_strip_suffix_not_present() {
        let full_name = "summary";
        let stripped = full_name.strip_suffix("_emb").unwrap_or(full_name);
        assert_eq!(stripped, "summary");
    }

    #[test]
    fn test_export_stats_creation() {
        let stats = ExportStats {
            stores: 5,
            embeddings: 1000,
        };

        assert_eq!(stats.stores, 5);
        assert_eq!(stats.embeddings, 1000);
    }

    #[test]
    fn test_exported_embedding_store_creation() {
        let store = ExportedEmbeddingStore {
            node_type: "Document".to_string(),
            text_column: "content".to_string(),
            dimension: 1536,
            entries: vec![(Value::String("doc_1".to_string()), vec![0.1, 0.2, 0.3])],
        };

        assert_eq!(store.node_type, "Document");
        assert_eq!(store.text_column, "content");
        assert_eq!(store.dimension, 1536);
        assert_eq!(store.entries.len(), 1);
    }

    #[test]
    fn test_exported_embedding_store_multiple_entries() {
        let store = ExportedEmbeddingStore {
            node_type: "Document".to_string(),
            text_column: "content".to_string(),
            dimension: 128,
            entries: vec![
                (Value::String("doc_1".to_string()), vec![0.1, 0.2]),
                (Value::String("doc_2".to_string()), vec![0.3, 0.4]),
                (Value::String("doc_3".to_string()), vec![0.5, 0.6]),
            ],
        };

        assert_eq!(store.entries.len(), 3);
        assert_eq!(store.dimension, 128);
    }

    #[test]
    fn test_exported_embedding_store_serde() {
        let store = ExportedEmbeddingStore {
            node_type: "Test".to_string(),
            text_column: "field".to_string(),
            dimension: 128,
            entries: vec![(Value::Int64(1), vec![0.5])],
        };

        let serialized = bincode_ser(&store).unwrap();
        let deserialized: ExportedEmbeddingStore = bincode_deser(&serialized).unwrap();

        assert_eq!(store.node_type, deserialized.node_type);
        assert_eq!(store.dimension, deserialized.dimension);
    }

    #[test]
    fn test_file_metadata_auto_vacuum_deserialize_default() {
        // Test that the serde default is correctly applied during deserialization
        let json = "{}";
        let metadata: FileMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(metadata.auto_vacuum_threshold, Some(0.3));
    }

    #[test]
    fn test_file_metadata_auto_vacuum_none() {
        let mut metadata = FileMetadata::default();
        metadata.auto_vacuum_threshold = None;

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: FileMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.auto_vacuum_threshold, None);
    }

    #[test]
    fn test_file_metadata_column_sections() {
        let section = V3ColumnSection {
            type_name: "Node".to_string(),
            compressed_size: 1024,
            row_count: 100,
            columns: HashMap::new(),
        };

        let mut metadata = FileMetadata::default();
        metadata.column_sections = vec![section.clone()];

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: FileMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.column_sections.len(), 1);
        assert_eq!(deserialized.column_sections[0].type_name, "Node");
    }

    #[test]
    fn test_file_metadata_connection_type_metadata() {
        let mut metadata = FileMetadata::default();
        // Note: ConnectionTypeInfo is defined elsewhere, so we just test the empty state
        assert!(metadata.connection_type_metadata.is_empty());

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: FileMetadata = serde_json::from_str(&json).unwrap();

        assert!(deserialized.connection_type_metadata.is_empty());
    }

    #[test]
    fn test_file_metadata_default() {
        let metadata = FileMetadata::default();

        assert_eq!(metadata.core_data_version, 0);
        assert_eq!(metadata.library_version, "");
        assert!(metadata.schema_definition.is_none());
        assert!(metadata.property_index_keys.is_empty());
        assert!(metadata.node_type_metadata.is_empty());
    }

    #[test]
    fn test_file_metadata_id_field_aliases() {
        let mut metadata = FileMetadata::default();
        metadata
            .id_field_aliases
            .insert("Person".to_string(), "person_id".to_string());

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: FileMetadata = serde_json::from_str(&json).unwrap();

        assert!(deserialized.id_field_aliases.contains_key("Person"));
    }

    #[test]
    fn test_file_metadata_node_type_metadata() {
        let mut metadata = FileMetadata::default();
        let mut type_meta = HashMap::new();
        type_meta.insert("name".to_string(), "string".to_string());
        metadata
            .node_type_metadata
            .insert("Person".to_string(), type_meta);

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: FileMetadata = serde_json::from_str(&json).unwrap();

        assert!(deserialized.node_type_metadata.contains_key("Person"));
    }

    #[test]
    fn test_file_metadata_serde_empty() {
        let metadata = FileMetadata::default();
        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: FileMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(metadata.core_data_version, deserialized.core_data_version);
        assert_eq!(metadata.library_version, deserialized.library_version);
    }

    #[test]
    fn test_file_metadata_spatial_configs() {
        let mut metadata = FileMetadata::default();
        assert!(metadata.spatial_configs.is_empty());

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: FileMetadata = serde_json::from_str(&json).unwrap();

        assert!(deserialized.spatial_configs.is_empty());
    }

    #[test]
    fn test_file_metadata_temporal_configs() {
        let mut metadata = FileMetadata::default();
        assert!(metadata.temporal_node_configs.is_empty());
        assert!(metadata.temporal_edge_configs.is_empty());

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: FileMetadata = serde_json::from_str(&json).unwrap();

        assert!(deserialized.temporal_node_configs.is_empty());
        assert!(deserialized.temporal_edge_configs.is_empty());
    }

    #[test]
    fn test_file_metadata_timeseries_configs() {
        let mut metadata = FileMetadata::default();
        assert!(metadata.timeseries_configs.is_empty());

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: FileMetadata = serde_json::from_str(&json).unwrap();

        assert!(deserialized.timeseries_configs.is_empty());
    }

    #[test]
    fn test_file_metadata_title_field_aliases() {
        let mut metadata = FileMetadata::default();
        metadata
            .title_field_aliases
            .insert("Company".to_string(), "company_name".to_string());

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: FileMetadata = serde_json::from_str(&json).unwrap();

        assert!(deserialized.title_field_aliases.contains_key("Company"));
    }

    #[test]
    fn test_file_metadata_with_values() {
        let mut metadata = FileMetadata::default();
        metadata.core_data_version = 1;
        metadata.library_version = "0.6.5".to_string();
        metadata.topology_compressed_size = 2048;
        metadata.timeseries_data_version = 2;

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: FileMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.core_data_version, 1);
        assert_eq!(deserialized.library_version, "0.6.5");
        assert_eq!(deserialized.topology_compressed_size, 2048);
        assert_eq!(deserialized.timeseries_data_version, 2);
    }

    #[test]
    fn test_file_mmap_threshold() {
        assert_eq!(FILE_MMAP_THRESHOLD, 65536); // 64 KB
    }

    #[test]
    fn test_import_stats_creation() {
        let stats = ImportStats {
            stores: 3,
            imported: 500,
            skipped: 100,
        };

        assert_eq!(stats.stores, 3);
        assert_eq!(stats.imported, 500);
        assert_eq!(stats.skipped, 100);
    }

    #[test]
    fn test_kgle_magic_bytes() {
        assert_eq!(KGLE_MAGIC.len(), 4);
        assert_eq!(KGLE_MAGIC, *b"KGLE");
    }

    #[test]
    fn test_kgle_version() {
        assert_eq!(KGLE_VERSION, 1);
    }

    #[test]
    fn test_large_le_bytes_roundtrip() {
        let original: u32 = 0xDEADBEEF;
        let bytes = original.to_le_bytes();
        let restored = u32::from_le_bytes(bytes);

        assert_eq!(original, restored);
    }

    #[test]
    fn test_le_bytes_roundtrip() {
        let original: u32 = 42;
        let bytes = original.to_le_bytes();
        let restored = u32::from_le_bytes(bytes);

        assert_eq!(original, restored);
    }

    #[test]
    fn test_u32_from_le_bytes() {
        let bytes = [0x78, 0x56, 0x34, 0x12];
        let value = u32::from_le_bytes(bytes);

        assert_eq!(value, 0x12345678);
    }

    #[test]
    fn test_u32_to_le_bytes() {
        let value: u32 = 0x12345678;
        let bytes = value.to_le_bytes();

        assert_eq!(bytes[0], 0x78);
        assert_eq!(bytes[1], 0x56);
        assert_eq!(bytes[2], 0x34);
        assert_eq!(bytes[3], 0x12);
    }

    #[test]
    fn test_v3_column_section_creation() {
        let section = V3ColumnSection {
            type_name: "Node".to_string(),
            compressed_size: 1024,
            row_count: 100,
            columns: {
                let mut map = HashMap::new();
                map.insert("name".to_string(), "string".to_string());
                map.insert("count".to_string(), "int64".to_string());
                map
            },
        };

        assert_eq!(section.type_name, "Node");
        assert_eq!(section.compressed_size, 1024);
        assert_eq!(section.row_count, 100);
        assert_eq!(section.columns.len(), 2);
    }

    #[test]
    fn test_v3_column_section_empty_columns() {
        let section = V3ColumnSection {
            type_name: "EmptyType".to_string(),
            compressed_size: 0,
            row_count: 0,
            columns: HashMap::new(),
        };

        assert_eq!(section.columns.len(), 0);
        assert_eq!(section.row_count, 0);
    }

    #[test]
    fn test_v3_column_section_serde() {
        let section = V3ColumnSection {
            type_name: "Test".to_string(),
            compressed_size: 512,
            row_count: 50,
            columns: {
                let mut map = HashMap::new();
                map.insert("field".to_string(), "type".to_string());
                map
            },
        };

        let json = serde_json::to_string(&section).unwrap();
        let deserialized: V3ColumnSection = serde_json::from_str(&json).unwrap();

        assert_eq!(section.type_name, deserialized.type_name);
        assert_eq!(section.compressed_size, deserialized.compressed_size);
        assert_eq!(section.row_count, deserialized.row_count);
    }

    #[test]
    fn test_v3_magic_bytes() {
        assert_eq!(V3_MAGIC.len(), 4);
        assert_eq!(V3_MAGIC[0], 0x52); // 'R'
        assert_eq!(V3_MAGIC[1], 0x47); // 'G'
        assert_eq!(V3_MAGIC[2], 0x46); // 'F'
        assert_eq!(V3_MAGIC[3], 0x03); // v3
    }

    #[test]
    fn test_zstd_compress_decompress_symmetry() {
        let original = b"The quick brown fox jumps over the lazy dog";
        let compressed = zstd_compress(original).unwrap();
        let decompressed = zstd_decompress(&compressed).unwrap();

        assert_eq!(original.to_vec(), decompressed);
    }

    #[test]
    fn test_zstd_compress_empty_data() {
        let original = b"";
        let compressed = zstd_compress(original).unwrap();
        let decompressed = zstd_decompress(&compressed).unwrap();

        assert_eq!(original.to_vec(), decompressed);
    }

    #[test]
    fn test_zstd_compress_json() {
        let json = r#"{"name": "test", "value": 42}"#;
        let compressed = zstd_compress(json.as_bytes()).unwrap();
        let decompressed = zstd_decompress(&compressed).unwrap();

        assert_eq!(json.as_bytes().to_vec(), decompressed);
    }

    #[test]
    fn test_zstd_compress_large_data() {
        let original = vec![42u8; 100000];
        let compressed = zstd_compress(&original).unwrap();
        let decompressed = zstd_decompress(&compressed).unwrap();

        assert_eq!(original, decompressed);
    }

    #[test]
    fn test_zstd_compress_repetitive_data() {
        // Use larger repetitive data to ensure compression ratio is good
        let mut original = Vec::new();
        for _ in 0..1000 {
            original.extend_from_slice(b"aaa");
        }

        let compressed = zstd_compress(&original).unwrap();

        // Highly repetitive large data should compress significantly
        assert!(compressed.len() < original.len());
    }

    #[test]
    fn test_zstd_decompress_invalid_data() {
        let invalid = vec![0xFF, 0xFF, 0xFF];
        let result = zstd_decompress(&invalid);

        assert!(result.is_err());
    }
}
