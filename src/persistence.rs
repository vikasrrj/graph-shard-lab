use crate::error::{GraphError, Result};
use crate::sharded::{ShardedGraph, parse_placement_info};
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub timestamp: u64,
    pub shard_count: usize,
    pub placement_info: String,
    pub users: Vec<SnapshotUser>,
    pub edges: Vec<SnapshotEdge>,
}

#[derive(Debug, Clone)]
pub struct SnapshotUser {
    pub id: u64,
    pub name: String,
    pub shard_id: usize,
}

#[derive(Debug, Clone)]
pub struct SnapshotEdge {
    pub source: u64,
    pub target: u64,
    pub source_shard: usize,
}

#[derive(Debug, Clone)]
pub struct OperationLogEntry {
    pub sequence: u64,
    pub operation: Operation,
}

#[derive(Debug, Clone)]
pub enum Operation {
    AddUser { id: u64, name: String },
    AddFollow { source: u64, target: u64 },
    RemoveFollow { source: u64, target: u64 },
}

#[derive(Debug, Clone)]
pub struct LogStats {
    pub total_operations: usize,
    pub add_user_ops: usize,
    pub add_follow_ops: usize,
    pub remove_follow_ops: usize,
}

pub fn create_snapshot(graph: &ShardedGraph, timestamp: u64) -> Snapshot {
    let mut users = Vec::new();
    let mut edges = Vec::new();

    for shard_id in 0..graph.shard_count() {
        let shard = &graph.shards[shard_id];

        for user_id in shard.user_ids() {
            if let Some(user) = shard.get_user(user_id) {
                users.push(SnapshotUser {
                    id: user.id,
                    name: user.name.clone(),
                    shard_id,
                });

                for &target in shard.get_following_ids(user_id) {
                    edges.push(SnapshotEdge {
                        source: user_id,
                        target,
                        source_shard: shard_id,
                    });
                }
            }
        }
    }

    Snapshot {
        timestamp,
        shard_count: graph.shard_count(),
        placement_info: graph.placement_info(),
        users,
        edges,
    }
}

pub fn save_snapshot(snapshot: &Snapshot, path: &Path) -> Result<()> {
    let file = fs::File::create(path).map_err(|e| GraphError::IoError(e.to_string()))?;

    let mut writer = BufWriter::new(file);

    writeln!(writer, "SNAPSHOT v2").map_err(|e| GraphError::IoError(e.to_string()))?;

    writeln!(writer, "timestamp:{}", snapshot.timestamp)
        .map_err(|e| GraphError::IoError(e.to_string()))?;

    writeln!(writer, "shard_count:{}", snapshot.shard_count)
        .map_err(|e| GraphError::IoError(e.to_string()))?;

    writeln!(writer, "placement:{}", snapshot.placement_info)
        .map_err(|e| GraphError::IoError(e.to_string()))?;

    writeln!(writer, "USERS").map_err(|e| GraphError::IoError(e.to_string()))?;

    for user in &snapshot.users {
        writeln!(writer, "{}|{}|{}", user.id, user.name, user.shard_id)
            .map_err(|e| GraphError::IoError(e.to_string()))?;
    }

    writeln!(writer, "EDGES").map_err(|e| GraphError::IoError(e.to_string()))?;

    for edge in &snapshot.edges {
        writeln!(
            writer,
            "{}|{}|{}",
            edge.source, edge.target, edge.source_shard
        )
        .map_err(|e| GraphError::IoError(e.to_string()))?;
    }

    writer
        .flush()
        .map_err(|e| GraphError::IoError(e.to_string()))?;

    Ok(())
}

pub fn load_snapshot(path: &Path) -> Result<Snapshot> {
    let file = fs::File::open(path).map_err(|e| GraphError::IoError(e.to_string()))?;

    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let _header = lines
        .next()
        .ok_or_else(|| GraphError::IoError("Missing snapshot header".to_string()))?
        .map_err(|e| GraphError::IoError(e.to_string()))?;

    let mut timestamp = 0u64;
    let mut shard_count = 0usize;
    let mut placement_info = String::from("Hash");
    let mut users = Vec::new();
    let mut edges = Vec::new();

    let mut section = String::new();

    for line in lines {
        let line = line.map_err(|e| GraphError::IoError(e.to_string()))?;

        if let Some(val) = line.strip_prefix("timestamp:") {
            timestamp = val
                .parse()
                .map_err(|e| GraphError::IoError(format!("Invalid timestamp: {e}")))?;
        } else if let Some(val) = line.strip_prefix("shard_count:") {
            shard_count = val
                .parse()
                .map_err(|e| GraphError::IoError(format!("Invalid shard_count: {e}")))?;
        } else if let Some(val) = line.strip_prefix("placement:") {
            placement_info = val.to_string();
        } else if line == "USERS" {
            section = "users".to_string();
        } else if line == "EDGES" {
            section = "edges".to_string();
        } else if section == "users" && !line.is_empty() {
            let parts: Vec<&str> = line.split('|').collect();
            if parts.len() >= 3 {
                let id = parts[0]
                    .parse()
                    .map_err(|e| GraphError::IoError(format!("Invalid user id: {e}")))?;
                let name = parts[1].to_string();
                let shard_id = parts[2]
                    .parse()
                    .map_err(|e| GraphError::IoError(format!("Invalid shard_id: {e}")))?;

                users.push(SnapshotUser { id, name, shard_id });
            }
        } else if section == "edges" && !line.is_empty() {
            let parts: Vec<&str> = line.split('|').collect();
            if parts.len() >= 3 {
                let source = parts[0]
                    .parse()
                    .map_err(|e| GraphError::IoError(format!("Invalid source: {e}")))?;
                let target = parts[1]
                    .parse()
                    .map_err(|e| GraphError::IoError(format!("Invalid target: {e}")))?;
                let source_shard = parts[2]
                    .parse()
                    .map_err(|e| GraphError::IoError(format!("Invalid source_shard: {e}")))?;

                edges.push(SnapshotEdge {
                    source,
                    target,
                    source_shard,
                });
            }
        }
    }

    Ok(Snapshot {
        timestamp,
        shard_count,
        placement_info,
        users,
        edges,
    })
}

pub fn restore_from_snapshot(snapshot: &Snapshot) -> Result<ShardedGraph> {
    let placement = parse_placement_info(&snapshot.placement_info)?;

    let mut graph = ShardedGraph::with_placement(snapshot.shard_count, placement)?;

    for user in &snapshot.users {
        let _ = graph.add_user(user.id, &user.name);
    }

    for edge in &snapshot.edges {
        let _ = graph.add_follow(edge.source, edge.target);
    }

    Ok(graph)
}

pub fn append_operation(log_path: &Path, sequence: u64, op: &Operation) -> Result<()> {
    let file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .map_err(|e| GraphError::IoError(e.to_string()))?;

    let mut writer = BufWriter::new(file);

    let op_str = match op {
        Operation::AddUser { id, name } => format!("ADD_USER|{id}|{name}"),
        Operation::AddFollow { source, target } => format!("ADD_FOLLOW|{source}|{target}"),
        Operation::RemoveFollow { source, target } => format!("REMOVE_FOLLOW|{source}|{target}"),
    };

    writeln!(writer, "{sequence}|{op_str}").map_err(|e| GraphError::IoError(e.to_string()))?;

    writer
        .flush()
        .map_err(|e| GraphError::IoError(e.to_string()))?;

    Ok(())
}

pub fn read_operation_log(log_path: &Path) -> Result<Vec<OperationLogEntry>> {
    if !log_path.exists() {
        return Ok(Vec::new());
    }

    let file = fs::File::open(log_path).map_err(|e| GraphError::IoError(e.to_string()))?;

    let reader = BufReader::new(file);
    let mut entries = Vec::new();

    for line in reader.lines() {
        let line = line.map_err(|e| GraphError::IoError(e.to_string()))?;

        if line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.splitn(4, '|').collect();

        if parts.len() < 2 {
            continue;
        }

        let sequence = parts[0]
            .parse()
            .map_err(|e| GraphError::IoError(format!("Invalid sequence: {e}")))?;

        let operation = match parts[1] {
            "ADD_USER" => {
                if parts.len() < 3 {
                    continue;
                }

                let id = parts[2]
                    .parse()
                    .map_err(|e| GraphError::IoError(format!("Invalid user id: {e}")))?;
                let name = if parts.len() > 3 {
                    parts[3].to_string()
                } else {
                    String::new()
                };

                Operation::AddUser { id, name }
            }

            "ADD_FOLLOW" => {
                if parts.len() < 4 {
                    continue;
                }

                let source = parts[2]
                    .parse()
                    .map_err(|e| GraphError::IoError(format!("Invalid source: {e}")))?;
                let target = parts[3]
                    .parse()
                    .map_err(|e| GraphError::IoError(format!("Invalid target: {e}")))?;

                Operation::AddFollow { source, target }
            }

            "REMOVE_FOLLOW" => {
                if parts.len() < 4 {
                    continue;
                }

                let source = parts[2]
                    .parse()
                    .map_err(|e| GraphError::IoError(format!("Invalid source: {e}")))?;
                let target = parts[3]
                    .parse()
                    .map_err(|e| GraphError::IoError(format!("Invalid target: {e}")))?;

                Operation::RemoveFollow { source, target }
            }

            _ => continue,
        };

        entries.push(OperationLogEntry {
            sequence,
            operation,
        });
    }

    Ok(entries)
}

pub fn replay_operation_log(
    graph: &mut ShardedGraph,
    log_path: &Path,
    from_sequence: u64,
) -> Result<LogStats> {
    let entries = read_operation_log(log_path)?;

    let mut stats = LogStats {
        total_operations: 0,
        add_user_ops: 0,
        add_follow_ops: 0,
        remove_follow_ops: 0,
    };

    for entry in entries {
        if entry.sequence < from_sequence {
            continue;
        }

        stats.total_operations += 1;

        match &entry.operation {
            Operation::AddUser { id, name } => {
                let _ = graph.add_user(*id, name);
                stats.add_user_ops += 1;
            }

            Operation::AddFollow { source, target } => {
                let _ = graph.add_follow(*source, *target);
                stats.add_follow_ops += 1;
            }

            Operation::RemoveFollow { source, target } => {
                let _ = graph.remove_follow(*source, *target);
                stats.remove_follow_ops += 1;
            }
        }
    }

    Ok(stats)
}

pub fn recover_from_snapshot_and_log(
    snapshot_path: &Path,
    log_path: &Path,
) -> Result<(ShardedGraph, LogStats)> {
    let snapshot = load_snapshot(snapshot_path)?;

    let mut graph = restore_from_snapshot(&snapshot)?;

    let stats = replay_operation_log(&mut graph, log_path, snapshot.timestamp)?;

    Ok((graph, stats))
}

pub fn verify_recovery(original: &ShardedGraph, recovered: &ShardedGraph) -> Result<()> {
    if original.shard_count() != recovered.shard_count() {
        return Err(GraphError::IoError(format!(
            "Shard count mismatch: {} vs {}",
            original.shard_count(),
            recovered.shard_count()
        )));
    }

    if original.user_count() != recovered.user_count() {
        return Err(GraphError::IoError(format!(
            "User count mismatch: {} vs {}",
            original.user_count(),
            recovered.user_count()
        )));
    }

    if original.edge_count() != recovered.edge_count() {
        return Err(GraphError::IoError(format!(
            "Edge count mismatch: {} vs {}",
            original.edge_count(),
            recovered.edge_count()
        )));
    }

    let mut orig_user_ids: Vec<u64> = original
        .user_ids_per_shard()
        .into_iter()
        .flatten()
        .collect();
    orig_user_ids.sort_unstable();

    for user_id in &orig_user_ids {
        let orig_user = original.get_user(*user_id);
        let rec_user = recovered.get_user(*user_id);

        if orig_user.is_none() != rec_user.is_none() {
            return Err(GraphError::IoError(format!(
                "User {} presence mismatch",
                user_id
            )));
        }

        if let Some(orig) = orig_user
            && let Some(rec) = rec_user
            && orig.name != rec.name
        {
            return Err(GraphError::IoError(format!(
                "User {} name mismatch: {} vs {}",
                user_id, orig.name, rec.name
            )));
        }

        let orig_edges = original.get_following_ids(*user_id);
        let rec_edges = recovered.get_following_ids(*user_id);

        let mut orig_sorted = orig_edges.to_vec();
        let mut rec_sorted = rec_edges.to_vec();

        orig_sorted.sort_unstable();
        rec_sorted.sort_unstable();

        if orig_sorted != rec_sorted {
            return Err(GraphError::IoError(format!(
                "Edges mismatch for user {}",
                user_id
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn build_test_graph() -> ShardedGraph {
        let mut graph = ShardedGraph::new(4).unwrap();

        for id in 1..=16 {
            graph.add_user(id, &format!("user-{id}")).unwrap();
        }

        for source in 1..=16 {
            for offset in 1..=3 {
                let target = ((source + offset - 1) % 16) + 1;
                graph.add_follow(source, target).unwrap();
            }
        }

        graph
    }

    #[test]
    fn snapshot_creates_valid_snapshot() {
        let graph = build_test_graph();

        let snapshot = create_snapshot(&graph, 12345);

        assert_eq!(snapshot.timestamp, 12345);
        assert_eq!(snapshot.shard_count, 4);
        assert!(!snapshot.users.is_empty());
        assert!(!snapshot.edges.is_empty());
    }

    #[test]
    fn save_and_load_snapshot_roundtrips() {
        let graph = build_test_graph();

        let snapshot = create_snapshot(&graph, 12345);

        let dir = std::env::temp_dir().join("graph_shard_test");
        fs::create_dir_all(&dir).unwrap();

        let path = dir.join("test_snapshot.txt");

        save_snapshot(&snapshot, &path).unwrap();

        let loaded = load_snapshot(&path).unwrap();

        assert_eq!(loaded.timestamp, snapshot.timestamp);
        assert_eq!(loaded.shard_count, snapshot.shard_count);
        assert_eq!(loaded.users.len(), snapshot.users.len());
        assert_eq!(loaded.edges.len(), snapshot.edges.len());

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn restore_from_snapshot_creates_valid_graph() {
        let graph = build_test_graph();

        let snapshot = create_snapshot(&graph, 12345);

        let restored = restore_from_snapshot(&snapshot).unwrap();

        assert_eq!(restored.shard_count(), 4);
        assert_eq!(restored.user_count(), graph.user_count());
    }

    #[test]
    fn operation_log_writes_and_reads() {
        let dir = std::env::temp_dir().join("graph_shard_test_log");
        fs::create_dir_all(&dir).unwrap();

        let log_path = dir.join("test.log");

        let op1 = Operation::AddUser {
            id: 1,
            name: "Alice".to_string(),
        };
        let op2 = Operation::AddFollow {
            source: 1,
            target: 2,
        };
        let op3 = Operation::RemoveFollow {
            source: 1,
            target: 2,
        };

        append_operation(&log_path, 1, &op1).unwrap();
        append_operation(&log_path, 2, &op2).unwrap();
        append_operation(&log_path, 3, &op3).unwrap();

        let entries = read_operation_log(&log_path).unwrap();

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[1].sequence, 2);
        assert_eq!(entries[2].sequence, 3);

        fs::remove_file(&log_path).unwrap();
    }

    #[test]
    fn replay_operation_log_applies_operations() {
        let dir = std::env::temp_dir().join("graph_shard_test_replay");
        fs::create_dir_all(&dir).unwrap();

        let log_path = dir.join("replay.log");

        let mut graph = ShardedGraph::new(4).unwrap();

        for id in 1..=8 {
            graph.add_user(id, &format!("user-{id}")).unwrap();
        }

        let op1 = Operation::AddFollow {
            source: 1,
            target: 2,
        };
        let op2 = Operation::AddFollow {
            source: 1,
            target: 3,
        };

        append_operation(&log_path, 1, &op1).unwrap();
        append_operation(&log_path, 2, &op2).unwrap();

        let stats = replay_operation_log(&mut graph, &log_path, 0).unwrap();

        assert_eq!(stats.total_operations, 2);
        assert_eq!(stats.add_follow_ops, 2);

        fs::remove_file(&log_path).unwrap();
    }

    #[test]
    fn recover_from_snapshot_and_log_works() {
        let dir = std::env::temp_dir().join("graph_shard_test_recover");
        fs::create_dir_all(&dir).unwrap();

        let snapshot_path = dir.join("snapshot.txt");
        let log_path = dir.join("recovery.log");

        let graph = build_test_graph();

        let snapshot = create_snapshot(&graph, 100);

        save_snapshot(&snapshot, &snapshot_path).unwrap();

        let op = Operation::AddFollow {
            source: 1,
            target: 16,
        };
        append_operation(&log_path, 101, &op).unwrap();

        let (recovered, stats) = recover_from_snapshot_and_log(&snapshot_path, &log_path).unwrap();

        assert_eq!(stats.total_operations, 1);
        assert_eq!(recovered.shard_count(), 4);

        fs::remove_file(&snapshot_path).unwrap();
        fs::remove_file(&log_path).unwrap();
    }

    #[test]
    fn verify_recovery_succeeds_for_identical_graphs() {
        let graph = build_test_graph();

        let snapshot = create_snapshot(&graph, 12345);

        let recovered = restore_from_snapshot(&snapshot).unwrap();

        verify_recovery(&graph, &recovered).unwrap();
    }

    #[test]
    fn verify_recovery_detects_mismatch() {
        let graph1 = build_test_graph();

        let mut graph2 = ShardedGraph::new(4).unwrap();

        for id in 1..=16 {
            graph2.add_user(id, &format!("user-{id}")).unwrap();
        }

        let result = verify_recovery(&graph1, &graph2);

        assert!(result.is_err());
    }
}
