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

my_func()