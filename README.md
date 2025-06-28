# 更新履歴
2025/6/20 レポジトリ公開  
2025/6/28 ダウンロードファイルが更新されないバグを修正

# Text Correction (Bedrock + CDK)

CloudShell でデプロイできる “テキスト校正アプリ” です。  
アップロードしたテキストを **Amazon Bedrock – Nova Micro** に渡してフィラーを除去し、ダウンロード可能なテキストファイルを生成します。

## デプロイ手順

まず Bedrockのモデルアクセス設定から、Nova Micro を有効化してください。
その後、CloudShellで以下のようにしてデプロイします。

```bash
# CloudShell
mkdir /tmp/project
cd /tmp/project

git clone https://github.com/nkknj/simplecorrect.git
cd simplecorrect

python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
sudo npm install -g aws-cdk@latest

export AWS_REGION=us-east-1

cd infra
cdk bootstrap aws://$(aws sts get-caller-identity --query Account --output text)/$AWS_REGION
cdk deploy --all

# API エンドポイントをコードに直接書き込んで再度デプロイ
STACK=TextCorrectionStack
API_URL=$(aws cloudformation describe-stacks --stack-name $STACK --query "Stacks[0].Outputs[?OutputKey=='ApiEndpoint'].OutputValue" --output text)
sed -i "s|__API_ENDPOINT__|${API_URL}|g" ../frontend/index.html
cdk deploy --all
```

表示されたFrontendURLにアクセスし、テキストファイルをアップロードすると、校正されたファイルがダウンロード可能な形で提供されます。

### 注意事項
- 一学習者が作成したコードであり、意図していないバグが存在する可能性があります。
- 本コードの利用により生じた損害等について、一切の責任を負いかねます。
