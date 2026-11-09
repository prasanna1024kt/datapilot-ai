
import json
from pathlib import Path

import yaml
from pypdf import PdfReader


SUPPORTED_EXTENSIONS = {".md", ".yaml", ".yml", ".json", ".pdf"}


def load_document(file_path: str) -> dict:
    path = Path(file_path)

    if not path.is_file():
        raise FileNotFoundError(f"Document not found: {path}")

    extension = path.suffix.lower()

    if extension not in SUPPORTED_EXTENSIONS:
        raise ValueError(f"Unsupported document format: {extension}")

    if extension == ".pdf":
        reader = PdfReader(str(path))
        content = "\n".join(
            page.extract_text() or "" for page in reader.pages
        )

    elif extension in {".yaml", ".yml"}:
        with path.open("r", encoding="utf-8") as file:
            content = yaml.safe_dump(
                yaml.safe_load(file),
                sort_keys=False,
                allow_unicode=True,
            )

    elif extension == ".json":
        with path.open("r", encoding="utf-8") as file:
            content = json.dumps(
                json.load(file),
                indent=2,
                ensure_ascii=False,
            )

    else:
        content = path.read_text(encoding="utf-8")

    return {
        "document_id": str(path.resolve()),
        "source_file": path.name,
        "document_type": extension.lstrip("."),
        "content": content,
        "character_count": len(content),
    }

def load_documents(directory: str) -> list[dict]:
    root = Path(directory).resolve()
    if not root.is_dir():
        raise NotADirectoryError(
            f"Directory not found: {root}"
        )
    documents = []

    for path in sorted(root.rglob("*")):
        if path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS:
            document = load_document(str(path))

            relative_path = path.relative_to(root).as_posix()

            document["document_id"] = relative_path
            document["source_file"] = relative_path

            documents.append(document)

    return documents

