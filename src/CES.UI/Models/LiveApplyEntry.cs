using System;

namespace CES.UI.Models;

// One processed event in a live consumer's log: applied to the destination DB,
// or detected as a duplicate via the ledger and skipped.
public record LiveApplyEntry(
    DateTime Time,
    string Operation,
    string Key,
    long SequenceNumber,
    bool IsApplied);
