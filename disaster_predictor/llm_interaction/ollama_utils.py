import ollama
import os
import threading
from dotenv import load_dotenv, dotenv_values
from pathlib import Path
import warnings

_ollama_client_lock = threading.Lock()
_ollama_clients: dict[str, ollama.Client] = {}


def _get_ollama_client(host: str):
	"""Reuse one HTTP client per host (fewer TLS handshakes; safe for concurrent generate calls)."""
	with _ollama_client_lock:
		if host not in _ollama_clients:
			_ollama_clients[host] = ollama.Client(host=host)
		return _ollama_clients[host]

# Load .env from project root (consistent with other modules)
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env", override=False)

def _airflow_dotenv_paths() -> tuple[Path, ...]:
	"""Paths that may define OLLAMA_* in Astro (container) or local include layouts."""
	p = Path(__file__).resolve()
	out: list[Path] = [Path("/usr/local/airflow/.env")]
	# .../airflow/include/llm_interaction/ollama_utils.py -> airflow/.env
	if p.parent.name == "llm_interaction" and len(p.parents) > 2 and p.parents[1].name == "include":
		out.append(p.parents[2] / ".env")
	# .../<project>/llm_interaction/ollama_utils.py -> project/.env
	out.append(p.parents[1] / ".env")
	# Unique preserve order
	seen: set[Path] = set()
	uniq: list[Path] = []
	for x in out:
		if x not in seen:
			seen.add(x)
			uniq.append(x)
	return tuple(uniq)


def _running_in_container() -> bool:
	return Path("/.dockerenv").is_file()


def _ollama_host_from_airflow_dotenv() -> str:
	"""Read OLLAMA_HOST from airflow .env without mutating os.environ (avoids clobbering K8s secrets)."""
	for p in _airflow_dotenv_paths():
		if p.is_file():
			val = (dotenv_values(p).get("OLLAMA_HOST") or "").strip()
			if val:
				return val
	return ""


def _resolve_ollama_host() -> str:
	"""
	Effective URL for the Ollama HTTP API.

	Inside Docker, localhost/127.0.0.1 points at the container, not the Mac/Linux host where Ollama runs.
	Docker Compose often forwards the *host* shell's OLLAMA_HOST=localhost into the container; we prefer
	airflow/.env when it defines a non-loopback URL.
	"""
	local_defaults = frozenset(
		{
			"http://localhost:11434",
			"http://127.0.0.1:11434",
			"localhost:11434",
			"127.0.0.1:11434",
		}
	)
	env_raw = (os.getenv("OLLAMA_HOST") or "").strip()
	file_raw = _ollama_host_from_airflow_dotenv()

	raw = env_raw
	if _running_in_container() and env_raw in local_defaults and file_raw:
		raw = file_raw
	elif not raw:
		raw = file_raw
	if not raw:
		raw = "http://localhost:11434"
	if _running_in_container() and raw in local_defaults:
		return "http://host.docker.internal:11434"
	return raw


def _resolve_ollama_model(model: str) -> str:
	env_m = (os.getenv("OLLAMA_MODEL") or "").strip()
	if env_m:
		return env_m
	for p in _airflow_dotenv_paths():
		if p.is_file():
			val = (dotenv_values(p).get("OLLAMA_MODEL") or "").strip()
			if val:
				return val
	return model or "mistral:7b"


def send_prompt_to_ollama(
	model,
	prompt,
	temperature=0.9,
	top_p=0.9,
	top_k=40,
	max_output_tokens=2048,
	num_ctx=None,
):
	"""
	Send prompt to a locally running Ollama model.
	
	Args:
		model: Ollama tag to use unless OLLAMA_MODEL is set in the environment.
		prompt: The text prompt to send to the model.
		temperature: Controls randomness (0.0-1.0). Default 0.9.
		top_p: Nucleus sampling parameter. Default 0.9.
		top_k: Top-k sampling parameter. Default 40.
		max_output_tokens: Maximum tokens to generate. Default 2048.
		num_ctx: KV cache / context length cap (smaller = faster for short prompts). None uses OLLAMA_NUM_CTX env if set, else model default.
	
	Returns:
		str: The generated response text.
	"""
	ollama_model = _resolve_ollama_model(model)
	ollama_host = _resolve_ollama_host()

	if num_ctx is None:
		env_ctx = (os.getenv("OLLAMA_NUM_CTX") or "").strip()
		if env_ctx:
			try:
				num_ctx = int(env_ctx)
			except ValueError:
				num_ctx = None
		if num_ctx is not None and num_ctx <= 0:
			num_ctx = None

	try:
		client = _get_ollama_client(ollama_host)

		options = {
			"temperature": temperature,
			"top_p": top_p,
			"top_k": top_k,
			"num_predict": max_output_tokens,
		}
		if num_ctx is not None:
			options["num_ctx"] = num_ctx

		response = client.generate(
			model=ollama_model,
			prompt=prompt,
			options=options,
		)
		
		# Extract response text
		response_text = response.get('response', '')
		
		# Check if response was truncated
		if response.get('done', False) is False:
			warnings.warn(
				f"Response may have been truncated. "
				f"Consider increasing max_output_tokens (current: {max_output_tokens}). "
				f"Response text: {response_text[:100]}..."
			)
		
		# Handle potential empty response
		if not response_text:
			warnings.warn("Empty response received from Ollama")
			return ""
		
		return response_text
		
	except Exception as e:
		warnings.warn(
			f"Error calling Ollama API at {ollama_host!r} (model={ollama_model!r}): {e}. "
			"If Airflow runs in Docker, use host.docker.internal (see docker-compose.override.yml) and ensure Ollama listens on the host."
		)
		raise
