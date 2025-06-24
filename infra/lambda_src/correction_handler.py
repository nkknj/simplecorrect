import os, json, urllib.parse, boto3
import string

BEDROCK_REGION = os.environ["BEDROCK_REGION"]
MODEL_ID = "amazon.nova-micro-v1:0"

s3 = boto3.client("s3")
bedrock = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)

def digitonly(text):
    for char in text:
        if char not in string.punctuation and not char.isdigit() and char != ' ':
            return False
    return True

def extract_in_flags(text, START='【Start】', END='【End】'):
    if not START in text:
        return text.split(END)[0]
    if not END in text:
        return text.split(START)[1]
    cand = text.split(START)
    cand = [c.split(END)[0] for c in cand if END in c]
    if len(cand) == 0:
        return text
    elif len(cand) == 1:
        return cand[0]
    else:
        max_length = 0
        out = ''
        for c in cand:
            if len(c) > max_length:
                out = c
                max_length = len(c)
        return out

def correct_text(text):
    corrected = remove_filler(text)
    return corrected

def remove_filler(text, MODEL_ID=MODEL_ID):
    user_message = f"次に示す文章から、「あー」「えー」などのフィラーを削除したものを、【Start】【End】で括って出力してください。\n\n{text}"
    resp = bedrock.converse(
            modelId=MODEL_ID,
            messages=[{
                "role": "user",
                "content": [{"text": user_message}]
            }],
            inferenceConfig={"maxTokens": 800},
    )
    corrected = resp["output"]["message"]["content"][0]["text"]
    corrected = extract_in_flags(corrected)
    return corrected    

def handler(event, _):
    for rec in event["Records"]:
        bucket = rec["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])
        if not key.startswith("uploads/"):
            continue  # ガード

        obj = s3.get_object(Bucket=bucket, Key=key)
        text = obj["Body"].read().decode()
        textlist = text.split('\n')

        outputtext = ''

        for t in textlist:
            if digitonly(t):
                outputtext = outputtext + t + '\n'
            else:
                corrected = correct_text(t)
                outputtext = outputtext + corrected + '\n'

        out_key = "outputs/correction.txt"
        s3.put_object(Bucket=bucket, Key=out_key,
                      Body=outputtext.encode("utf-8"),
                      ContentType="text/plain")
