import os
import urllib.parse
from google.cloud import pubsub_v1
from flask import jsonify, request

publisher = pubsub_v1.PublisherClient()

def my_cloud_function(request):
    allowed_origin = 'https://mahmoudrda.github.io'
    request_origin = request.headers.get('Origin')
    request_headers = request.headers.get('Access-Control-Request-Headers', '')

    # Set CORS headers
    headers = {
        'Access-Control-Allow-Headers': request_headers,
        'Access-Control-Allow-Methods': 'POST, OPTIONS'
    }
    if request_origin == allowed_origin:
        headers['Access-Control-Allow-Origin'] = allowed_origin

    # Handle preflight CORS
    if request.method == 'OPTIONS':
        return ('', 204, headers)

    # Enforce allowed origin
    if request_origin != allowed_origin:
        return (jsonify({'error': 'Unauthorized origin'}), 403, headers)

    # Accept only POST
    if request.method != 'POST':
        return (jsonify({'error': 'Method not allowed'}), 405, headers)

    # Parse form-encoded POST body
    form_data = request.form.to_dict()
    if not form_data:
        return (jsonify({'error': 'No form data provided'}), 400, headers)

    print(f'Received form data: {form_data}')

    # Convert to query string format (GA-style)
    message_string = urllib.parse.urlencode(form_data)
    message_bytes = message_string.encode('utf-8')

    topic_path = 'projects/idyllic-ethos-464713-j5/topics/analytics'

    try:
        publish_future = publisher.publish(topic_path, data=message_bytes)
        publish_future.result()

        return (jsonify({'status': 'success', 'published': message_string}), 200, headers)

    except Exception as e:
        print(f'Error publishing to Pub/Sub: {e}')
        return (jsonify({'status': 'error', 'message': str(e)}), 500, headers)
