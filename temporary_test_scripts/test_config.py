from config.config import settings
import json

print(json.dumps(settings.model_dump(), indent=2))
