from typing import List, Dict, Any
from pathlib import Path
import json
import shutil


def save_to_disk(json_content: List[Dict[str, Any]], path: str) -> None:
    
    target_dir = Path(path)
    
    if target_dir.exists():
        print(f"Removing existing directory: {target_dir}")
        shutil.rmtree(target_dir)

    target_dir.mkdir(parents=True, exist_ok=True)
    print(f"Created directory: {target_dir}")

    date = target_dir.name
    filename = f"sales_{date}.json"
    filepath = target_dir / filename

    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(json_content, f, indent=2, ensure_ascii=False)
    
    print(f"Saved {len(json_content)} records to {filepath}")
    
    pass
