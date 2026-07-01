using System;

namespace CES.UI.Models;

public record LedgerEntry(
    int PartitionId,
    long SequenceNumber,
    string CommitLsn,
    DateTime ProcessedAt
);

public record OffsetEntry(
    int PartitionId,
    long LastSequenceNumber,
    string LastCommitLsn,
    DateTime UpdatedAt
);
