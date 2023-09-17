import requests
from requests.structures import CaseInsensitiveDict

def my_func(request):
	print("slack with gitwebhooks")
	url = "https://hooks.slack.com/services/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"
	headers = CaseInsensitiveDict()
	headers["Content-type"]= "application/json"
	data = '{"text":"GITHUB code is committed"}'
	resp = requests.post(url,headers=headers, data=data)
	print(resp.status_code)
	return f"Hellow World" 