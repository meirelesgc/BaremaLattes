from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str
    ADMIN_DATABASE_URL: str

    ALTERNATIVE_CNPQ_SERVICE: bool = False

    XML_PATH: str = "storage/xml"
    CURRENT_XML_PATH: str = "storage/xml/current"
    ZIP_XML_PATH: str = "storage/xml/current"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
