from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from app.core.match_state import MatchState
from app.v2.runtime.runtime_cycle_v2 import RuntimeCycleV2


class LiveShadowBridge:
    """
    Documentary bridge that runs V2 on live MatchState snapshots and records
    live-shadow comparisons against optional V1 documentary references.
    """

    source_mode = "live_shadow"

    def __init__(
        self,
        runtime: RuntimeCycleV2 | None = None,
        *,
        export_path: str | Path | None = None,
    ) -> None:
        if runtime is None:
            self.runtime = RuntimeCycleV2(export_path=export_path or "exports/v2/live_shadow_bridge.jsonl")
        else:
            self.runtime = runtime
            if export_path is not None:
                self.runtime.export_path = Path(export_path)

    def run_live_states(
        self,
        states: Iterable[MatchState],
        *,
        v1_match_documents: dict[int, Any] | None = None,
        v1_board_best: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = self.runtime.run_states(
            states,
            v1_match_documents=v1_match_documents,
            v1_board_best=v1_board_best,
            source_mode=self.source_mode,
        )
        payload["event"] = "live_shadow_bridge"
        payload["source_mode"] = self.source_mode
        return payload
