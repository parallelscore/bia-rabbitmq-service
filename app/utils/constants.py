AI_ANALYSIS_QUEUE = 'insomnia'

RABBITMQ_QUEUES = {
  AI_ANALYSIS_QUEUE: {
    'subscribers': [
      {
        'method': 'queue', # 'endpoint'|'queue'
        'address': 'bia_subscribe_queue', # 'POST url'|'queue name'
      }
    ]
  },
  'bia_publish_queue': {
    'subscribers': [
      {
        'method': 'endpoint', # 'endpoint'|'queue'
        'address': 'POST http://localhost:3000/rabbitmq_consumer', # use nestjs sandbox # 'POST url'|'queue name'
      }
    ]
  }
}
