"""Process 01 application composition and its sole Prefect observation boundary."""

from pathlib import Path

from prefect import flow

from .shared_database import load_database_configuration
from .p01_02_import_execution import convert_series, execute_local
from .p01_03_gift_eval_source import GiftEvalSource
from .p01_05_import_store import ImportStore


class ImportProcess:
    """Coordinate canonical inputs, execution and durable item commits."""

    def __init__(self, source, store, execute=execute_local):
        self.source = source
        self.store = store
        self.execute = execute

    def run(self):
        """Resume only unfinished work; final acceptance belongs to the store."""
        accepted = self.store.completed_summary()
        if accepted is not None:
            return {"process_id": 1, "status": "skipped_completed", "summary": accepted}
        try:
            self.store.begin(self.source.fingerprint(), self.source.metadata())
            completed, _ = self.store.item_states()
            for record in self.source.records(exclude=completed):
                self.store.start_item(record.source_series_id)
                result = self.execute(convert_series, record, self.source.settings)
                self.store.accept(result)
            summary = self.store.finish(len(completed))
            return {"process_id": 1, "status": "completed", "summary": summary}
        except BaseException as error:
            self.store.fail(error)
            raise


@flow(name="Process 01 Import", persist_result=False)
def import_flow(database, configuration=None):
    """Expose one process flow; no task/subflow per internal method or item."""
    configuration = configuration or load_database_configuration(database)
    source = GiftEvalSource(configuration, Path(__file__).resolve().parents[3])
    with ImportStore(database, configuration) as store:
        return ImportProcess(source, store).run()
