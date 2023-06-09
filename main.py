import hashlib
import logging
import sys
import pdb

from turbine.src.turbine_app import RecordList, TurbineApp

logging.basicConfig(level=logging.INFO)


def transform(records: RecordList) -> RecordList:
    print("Inside Tranform for west-store-to-atlas")
    logging.info(f"processing {len(records)} record(s)")
    for record in records:
        print("Inside Tranform FOR LOOP for west-store-to-atlas")
        logging.info(f"input: {record}")
        try:
            record.value["payload"]["store_id"] = "002"
            record.value["payload"]["store_location"] = "west"

            record.value["payload"]["after"]["store_id"] = "002"
            record.value["payload"]["after"]["store_location"] = "west"

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

            transformed = await turbine.process(records, transform)
            
            destination_db = await turbine.resources("mongo-atlas")

            await destination_db.write(transformed, "dispensed_pills_from_west_store", {
                "transforms": "unwrap",
                "transforms.unwrap.type": "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState"
            })
        except Exception as e:
            print(e, file=sys.stderr)
