import google.generativeai as genai
import os
from dotenv import load_dotenv

load_dotenv()

def send_prompt_to_gemini(model, prompt, temperature=0.9, top_p=0.9, top_k=40, max_output_tokens=2048):

	genai.configure(api_key = os.getenv('gemini_api_key'))
	model = genai.GenerativeModel(f'models/{model}')
	
	generation_config = {
		"temperature": temperature,
		"top_p": top_p,
		"top_k": top_k,
		"max_output_tokens": max_output_tokens
	}

	response = model.generate_content(
		contents=prompt,
		generation_config=generation_config
	)

	return response.text