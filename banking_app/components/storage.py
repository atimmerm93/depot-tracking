from pydantic_settings import BaseSettings


class ProcessedFile(BaseSettings):
    file_path: str
    file_hash: str
    parser_version: str = "v1"

