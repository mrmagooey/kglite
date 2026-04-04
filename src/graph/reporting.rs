use std::collections::VecDeque;

// Maximum number of reports to keep in history
const MAX_REPORT_HISTORY: usize = 10;

#[derive(Debug, Clone)]
#[allow(clippy::enum_variant_names)]
pub enum OperationReport {
    NodeOperation(NodeOperationReport),
    ConnectionOperation(ConnectionOperationReport),
    CalculationOperation(CalculationOperationReport),
}

#[derive(Debug, Clone)]
pub struct CalculationOperationReport {
    pub operation_type: String,
    pub expression: String,
    pub nodes_processed: usize,
    pub nodes_updated: usize,
    pub nodes_with_errors: usize,
    pub processing_time_ms: f64,
    pub is_aggregation: bool,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub errors: Vec<String>, // Add error collection
}

impl CalculationOperationReport {
    pub fn new(
        operation_type: String,
        expression: String,
        nodes_processed: usize,
        nodes_updated: usize,
        nodes_with_errors: usize,
        processing_time_ms: f64,
        is_aggregation: bool,
    ) -> Self {
        Self {
            operation_type,
            expression,
            nodes_processed,
            nodes_updated,
            nodes_with_errors,
            processing_time_ms,
            is_aggregation,
            timestamp: chrono::Utc::now(),
            errors: Vec::new(),
        }
    }

    pub fn with_errors(mut self, errors: Vec<String>) -> Self {
        self.errors = errors;
        self
    }
}

#[derive(Debug, Clone)]
pub struct NodeOperationReport {
    pub operation_type: String,
    pub nodes_created: usize,
    pub nodes_updated: usize,
    pub nodes_skipped: usize,
    pub processing_time_ms: f64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub errors: Vec<String>, // Add error collection
}

impl NodeOperationReport {
    pub fn new(
        operation_type: String,
        nodes_created: usize,
        nodes_updated: usize,
        nodes_skipped: usize,
        processing_time_ms: f64,
    ) -> Self {
        Self {
            operation_type,
            nodes_created,
            nodes_updated,
            nodes_skipped,
            processing_time_ms,
            timestamp: chrono::Utc::now(),
            errors: Vec::new(),
        }
    }

    pub fn with_errors(mut self, errors: Vec<String>) -> Self {
        self.errors = errors;
        self
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionOperationReport {
    pub operation_type: String,
    pub connections_created: usize,
    pub connections_skipped: usize,
    pub property_fields_tracked: usize,
    pub processing_time_ms: f64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub errors: Vec<String>, // Add error collection
}

impl ConnectionOperationReport {
    pub fn new(
        operation_type: String,
        connections_created: usize,
        connections_skipped: usize,
        property_fields_tracked: usize,
        processing_time_ms: f64,
    ) -> Self {
        Self {
            operation_type,
            connections_created,
            connections_skipped,
            property_fields_tracked,
            processing_time_ms,
            timestamp: chrono::Utc::now(),
            errors: Vec::new(),
        }
    }

    pub fn with_errors(mut self, errors: Vec<String>) -> Self {
        self.errors = errors;
        self
    }
}

// Create a reports container that we'll store in the KnowledgeGraph
#[derive(Debug, Clone, Default)]
pub struct OperationReports {
    reports: VecDeque<OperationReport>,
    last_operation_index: usize,
}

impl OperationReports {
    pub fn new() -> Self {
        OperationReports {
            reports: VecDeque::with_capacity(MAX_REPORT_HISTORY),
            last_operation_index: 0,
        }
    }

    pub fn add_report(&mut self, report: OperationReport) -> usize {
        // Increment the operation index
        self.last_operation_index += 1;

        // Add to the reports queue
        self.reports.push_back(report);

        // Remove oldest if we exceed the max history
        if self.reports.len() > MAX_REPORT_HISTORY {
            self.reports.pop_front();
        }

        // Return the operation index
        self.last_operation_index
    }

    pub fn get_last_report(&self) -> Option<&OperationReport> {
        self.reports.back()
    }

    pub fn get_all_reports(&self) -> &VecDeque<OperationReport> {
        &self.reports
    }

    pub fn get_last_operation_index(&self) -> usize {
        self.last_operation_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_operation_report_new() {
        let report = NodeOperationReport::new("add_nodes".to_string(), 5, 0, 0, 10.5);
        assert_eq!(report.operation_type, "add_nodes");
        assert_eq!(report.nodes_created, 5);
        assert_eq!(report.nodes_updated, 0);
        assert_eq!(report.nodes_skipped, 0);
        assert_eq!(report.processing_time_ms, 10.5);
        assert!(report.errors.is_empty());
    }

    #[test]
    fn test_node_operation_report_with_errors() {
        let errors = vec!["error1".to_string(), "error2".to_string()];
        let report = NodeOperationReport::new("add_nodes".to_string(), 5, 0, 0, 10.5)
            .with_errors(errors.clone());

        assert_eq!(report.errors, errors);
    }

    #[test]
    fn test_connection_operation_report_new() {
        let report = ConnectionOperationReport::new("add_connections".to_string(), 10, 2, 3, 20.5);
        assert_eq!(report.operation_type, "add_connections");
        assert_eq!(report.connections_created, 10);
        assert_eq!(report.connections_skipped, 2);
        assert_eq!(report.property_fields_tracked, 3);
        assert_eq!(report.processing_time_ms, 20.5);
        assert!(report.errors.is_empty());
    }

    #[test]
    fn test_connection_operation_report_with_errors() {
        let errors = vec!["connection error".to_string()];
        let report = ConnectionOperationReport::new("add_connections".to_string(), 10, 2, 3, 20.5)
            .with_errors(errors.clone());

        assert_eq!(report.errors, errors);
    }

    #[test]
    fn test_calculation_operation_report_new() {
        let report = CalculationOperationReport::new(
            "calculate".to_string(),
            "x + y".to_string(),
            100,
            95,
            5,
            15.0,
            false,
        );
        assert_eq!(report.operation_type, "calculate");
        assert_eq!(report.expression, "x + y");
        assert_eq!(report.nodes_processed, 100);
        assert_eq!(report.nodes_updated, 95);
        assert_eq!(report.nodes_with_errors, 5);
        assert_eq!(report.processing_time_ms, 15.0);
        assert!(!report.is_aggregation);
        assert!(report.errors.is_empty());
    }

    #[test]
    fn test_calculation_operation_report_aggregation() {
        let report = CalculationOperationReport::new(
            "aggregate_sum".to_string(),
            "SUM(values)".to_string(),
            50,
            50,
            0,
            5.0,
            true,
        );
        assert!(report.is_aggregation);
    }

    #[test]
    fn test_calculation_operation_report_with_errors() {
        let errors = vec!["calc error 1".to_string(), "calc error 2".to_string()];
        let report = CalculationOperationReport::new(
            "calculate".to_string(),
            "x / y".to_string(),
            100,
            95,
            5,
            15.0,
            false,
        )
        .with_errors(errors.clone());

        assert_eq!(report.errors, errors);
    }

    #[test]
    fn test_operation_reports_new() {
        let reports = OperationReports::new();
        assert_eq!(reports.get_last_operation_index(), 0);
        assert!(reports.get_last_report().is_none());
        assert!(reports.get_all_reports().is_empty());
    }

    #[test]
    fn test_operation_reports_add_single() {
        let mut reports = OperationReports::new();
        let report = NodeOperationReport::new("test".to_string(), 1, 0, 0, 1.0);
        let index = reports.add_report(OperationReport::NodeOperation(report));

        assert_eq!(index, 1);
        assert_eq!(reports.get_last_operation_index(), 1);
        assert!(reports.get_last_report().is_some());
        assert_eq!(reports.get_all_reports().len(), 1);
    }

    #[test]
    fn test_operation_reports_add_multiple() {
        let mut reports = OperationReports::new();

        let report1 = NodeOperationReport::new("op1".to_string(), 1, 0, 0, 1.0);
        let report2 = ConnectionOperationReport::new("op2".to_string(), 2, 0, 0, 2.0);

        let idx1 = reports.add_report(OperationReport::NodeOperation(report1));
        let idx2 = reports.add_report(OperationReport::ConnectionOperation(report2));

        assert_eq!(idx1, 1);
        assert_eq!(idx2, 2);
        assert_eq!(reports.get_last_operation_index(), 2);
        assert_eq!(reports.get_all_reports().len(), 2);
    }

    #[test]
    fn test_operation_reports_max_history() {
        let mut reports = OperationReports::new();

        // Add more than MAX_REPORT_HISTORY reports
        for i in 0..15 {
            let report = NodeOperationReport::new(format!("op{}", i), i as usize, 0, 0, 1.0);
            reports.add_report(OperationReport::NodeOperation(report));
        }

        // Should only keep the last MAX_REPORT_HISTORY (10)
        assert_eq!(reports.get_all_reports().len(), MAX_REPORT_HISTORY);
        assert_eq!(reports.get_last_operation_index(), 15);
    }

    #[test]
    fn test_operation_reports_preserves_newest() {
        let mut reports = OperationReports::new();

        // Add 12 reports
        for i in 0..12 {
            let report = NodeOperationReport::new(format!("op{}", i), i as usize, 0, 0, 1.0);
            reports.add_report(OperationReport::NodeOperation(report));
        }

        // Should have last 10 reports (indices 2-11)
        if let Some(OperationReport::NodeOperation(last)) = reports.get_last_report() {
            assert_eq!(last.nodes_created, 11); // Last report had nodes_created=11
        } else {
            panic!("Expected NodeOperation report");
        }
    }
}
