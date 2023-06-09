import hashlib
import logging
import sys
import pdb

from turbine.src.turbine_app import RecordList, TurbineApp

logging.basicConfig(level=logging.INFO)


def anonymize(records: RecordList) -> RecordList:
    logging.info(f"processing {len(records)} record(s)")
    for record in records:
        logging.info(f"input: {record}")
        try:
            payload = record.value["payload"]["after"]

            # Hash the email
            payload["email"] = hashlib.sha256(
                payload["email"].encode("utf-8")
            ).hexdigest()

            logging.info(f"output: {record}")
        except Exception as e:
            print("Error occurred while parsing records: " + str(e))
            logging.info(f"output: {record}")
    return records


class App:
    @staticmethod
    async def run(turbine: TurbineApp):
        try:
            source = await turbine.resources("west-store-mongo")

            records = await source.records("medicine", {})

            # turbine.register_secrets("PWD")

            transformed = await turbine.process(records, anonymize)
            
            destination_db = await turbine.resources("mongo-atlas")

            await destination_db.write(transformed, "alldispensedmedicine", {
                "transforms": "unwrap",
                "transforms.unwrap.type": "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState"
            })
        except Exception as e:
            print(e, file=sys.stderr)
