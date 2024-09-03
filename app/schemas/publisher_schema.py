from typing import Optional, Dict, Any, ClassVar
from pydantic import BaseModel, ConfigDict


class PublisherSchema(BaseModel):
    queue_name: Optional[str]
    message: Optional[Dict[str, Any]]

    publisher_schema_config: ClassVar = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            'example': {
                'queue_name': 'name_queue',
                'message': {
                    'key_one': 'value_one',
                    'key_two': 'value_two',
                }
            }
        }
    )
