# This is the infomation about the requirement of this cloud function and steps to be followed in order to test this cloud function from locally and then deploy it on Google Cloud Platform
You can use the following command to deploy an HTTP-triggered Cloud Function using the gcloud CLI in a Windows Command Prompt (cmd):

gcloud functions deploy my-function-name ^
  --runtime python310 ^
  --trigger-http ^
  --allow-unauthenticated ^
  --entry-point hello_http

# Note:
Please make sure that 'gcloud sdk' is installed in your local machine before executing the above command.
# To test the above function locally you can use the following command:

functions-framework --target=hello_http --port=8080

