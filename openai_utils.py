import os
from pydoc import text

from openai import AzureOpenAI
from pydantic import BaseModel

OPENAI_API_KEY     = os.environ.get("OPENAI_API_KEY")
OPENAI_ENDPOINT    = os.environ.get("OPENAI_ENDPOINT")
OPENAI_MODEL       = os.environ.get("OPENAI_MODEL", "gpt-4.1")
OPENAI_API_VERSION = os.environ.get("OPENAI_API_VERSION", "2025-03-01-preview")

# OPENAI UTILS

def connect_to_openai():
    """Returns an OpenAI client"""

    if OPENAI_API_KEY is None or OPENAI_ENDPOINT is None:
        raise RuntimeError('OPENAI_API_KEY or OPENAI_ENDPOINT is not set in the environment')

    client = AzureOpenAI(
        api_version=OPENAI_API_VERSION,
        azure_endpoint=OPENAI_ENDPOINT,
        api_key=OPENAI_API_KEY
    )

    return client

def get_response(client, system_prompt: str, content: str):
    response = client.responses.create(
        model=OPENAI_MODEL,
        input = [
            {
                "role": "system",
                "content": system_prompt
            },
            {
                "role": "user",
                "content": content
            }
        ],
    )
    return response.output_text

def get_structured_response(client, system_prompt: str, content: str, output_format: BaseModel):
    response = client.responses.parse(
        model=OPENAI_MODEL,
        input = [
            {
                "role": "system",
                "content": system_prompt
            },
            {
                "role": "user",
                "content": content
            }
        ],
        text_format=output_format
    )
    return response.output_parsed