from app.core.config import settings

AI_ANALYSIS_QUEUE = 'bia_subscribe_queue'

RABBITMQ_QUEUES = {
  'listen_queues': {
    'bia_publish_queue': [
      {
        'method': 'endpoint', # 'endpoint'|'queue'
        'address': [ # make this a string if there are no conditions
          {
            'condition': 'requestType == dispatchDocument',
            'address': f'POST {settings.AI_SERVICE_BASE_URL}/api/v1/document_extraction_analysis_rag'
          },
          {
            'condition': 'requestType == dispatchQuestionnaire',
            'address': f'POST {settings.AI_SERVICE_BASE_URL}/api/v1/questionaire_rag'
          }
        ]
        # 'address': 'POST http://localhost:3000/rabbitmq_consumer', # use nestjs sandbox # 'POST url'|'queue name'
      }
    ],
    # 'bia_subscribe_queue': [
    #   {
    #     'method': 'endpoint', # 'endpoint'|'queue'
    #     'address': 'POST http://localhost:3000/rabbitmq_consumer', # use nestjs sandbox # 'POST url'|'queue name'
    #   }
    # ]
  },
}
