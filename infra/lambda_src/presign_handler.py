import os
import urllib.parse
import json
import boto3
from datetime import timedelta, datetime, timezone

BUCKET = os.environ["BUCKET_NAME"]
s3 = boto3.client("s3")

def _presigned_put(key: str, expires=300):
    return s3.generate_presigned_url(
        "put_object",
        Params={"Bucket": BUCKET, "Key": key, "ContentType": "text/plain"},
        ExpiresIn=expires,
    )

def _presigned_get(key: str, expires=300):
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": BUCKET,
                "Key": key,
                "ResponseContentType": "text/plain",
                "ResponseContentDisposition": f'attachment; filename="{key.split("/")[-1]}"'},
        ExpiresIn=expires,
    )

def handler(event, _):
    """
    /presign/upload?filename=foo.txt
    /presign/download?key=uploads/foo.txt
    """
    qs = event.get("queryStringParameters") or {}
    path = event["path"]

    # -------- Upload --------
    if path.endswith("/upload"):
        filename = qs.get("filename")
        if not filename:
            return _resp(400, {"message": "filename required"})
        key = f"uploads/{_safe(filename)}"
        url = _presigned_put(key)
        return _resp(200, {"url": url, "key": key})

    # -------- Download --------
    if path.endswith("/download"):
        key = qs.get("key")  # uploads/foo.txt
        if not key:
            return _resp(400, {"message": "key required"})
        out_key = "outputs/correction.txt"

        try:
            head_input  = s3.head_object(Bucket=BUCKET, Key=key)
            head_output = s3.head_object(Bucket=BUCKET, Key=out_key)
        except s3.exceptions.ClientError as e:
            # input または output が無ければ「まだ準備中」
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return _resp(404, {"message": "not ready"})
            raise

        # 出力がアップロード時刻より前ならまだ更新されていない
        if head_output["LastModified"] < head_input["LastModified"]:
            return _resp(404, {"message": "not ready"})

        url = _presigned_get(out_key)
        return _resp(200, {"url": url})

def _safe(name: str) -> str:
    return urllib.parse.quote_plus(name, safe="")

def _resp(code: int, body: dict):
    return {
        "statusCode": code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": "true"
        },
        "body": json.dumps(body, ensure_ascii=False),
    }
