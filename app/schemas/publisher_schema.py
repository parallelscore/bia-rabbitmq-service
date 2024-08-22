from pydantic import BaseModel
from typing import Dict, Any, Optional


class PublisherSchema(BaseModel):
    queue_name: Optional[str]
    message: Optional[Dict[str, Any]]

    class Config:
        from_attributes = True
        json_schema_extra = {
            'example': {
                'queue_name': 'name_queue',
                'message': {
                    'key_one': 'value_one',
                    'key_two': 'value_two',
                }
            }
        }
