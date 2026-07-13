import json
import os
import logging
import uuid
import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

sqs = boto3.client('sqs')
QUEUE_URL = os.environ.get("QUEUE_URL")

class ResponseFactory:
    @staticmethod
    def create(status_code, body_payload):
        return {
            "statusCode": status_code,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps(body_payload)
        }

def ingest_audit(event, context):
    correlation_id = str(uuid.uuid4())
    logger.info(json.dumps({
        "message": "API ingestion pipeline processing started",
        "correlation_id": correlation_id
    }))

    try:
        if not event.get("body"):
            return ResponseFactory.create(400, {"error": "Missing runtime body data context"})

        body = json.loads(event["body"])
        
        if "case_id" not in body or "document_type" not in body:
            return ResponseFactory.create(400, {"error": "Missing mandatory validation keys: case_id, document_type"})

        body["correlation_id"] = correlation_id

        sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(body)
        )

        return ResponseFactory.create(202, {
            "status": "ACCEPTED",
            "message": "Data successfully added to asynchronous backend stream",
            "correlation_id": correlation_id
        })

    except Exception as e:
        logger.error(json.dumps({
            "message": "Fatal tracking exception on ingest execution mapping",
            "error": str(e),
            "correlation_id": correlation_id
        }))
        return ResponseFactory.create(500, {"error": "Internal Processing Pipeline Failure"})


def process_audit_batch(event, context):
    batch_item_failures = []
    records = event.get("Records", [])

    logger.info(json.dumps({"message": f"Polled batch cycle running. Records processing: {len(records)}"}))

    for record in records:
        message_id = record.get("messageId")
        try:
            payload = json.loads(record.get("body", "{}"))
            c_id = payload.get("correlation_id", "MALFORMED_OR_MISSING_TRACE")

            logger.info(json.dumps({
                "message": "Parsing single item document properties",
                "message_id": message_id,
                "correlation_id": c_id,
                "case_id": payload.get("case_id")
            }))

            if payload.get("document_type") == "CORRUPTED":
                raise ValueError("Simulated legal document parsing error anomaly")

            logger.info(json.dumps({
                "message": "Data successfully saved into transaction datastore",
                "message_id": message_id,
                "correlation_id": c_id
            }))

        except Exception as error:
            logger.error(json.dumps({
                "message": "Graceful capture of error structural block within ongoing batch sequence",
                "message_id": message_id,
                "error": str(error)
            }))
            batch_item_failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": batch_item_failures}