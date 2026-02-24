import json
import os

import requests
from fcm_django.models import FCMDevice


def push_notification(request=None, notification_data=None, user=None):
    serverToken = os.getenv("FCM_SERVER_KEY_ADMIN")
    if serverToken and user:
        devices = FCMDevice.objects.filter(user__in=user).order_by("device_id", "-id")

        device_token_list = (
            devices.exclude(registration_id__isnull=True)
            .exclude(registration_id="null")
            .values_list("registration_id", flat=True)
        )
        device_token_list = list(set(device_token_list))

        def divide_chunks(l, n):
            for i in range(0, len(l), n):
                yield l[i : i + n]

        deviceTokenList = list(divide_chunks(device_token_list, 900))

        headers = {
            "Content-Type": "application/json",
            "Authorization": "key=" + serverToken,
        }
        response_data = []
        for fcm_token_list in deviceTokenList:
            body = {
                "content_available": True,
                "mutable_content": True,
                "notification": notification_data,
                "registration_ids": fcm_token_list,
                "priority": "high",
                "data": notification_data,
            }
            response = requests.post(
                "https://fcm.googleapis.com/fcm/send",
                headers=headers,
                data=json.dumps(body),
            )
            print(f"Push notification success: {response.content}")
            response_data.append(json.loads(response.content.decode()))
        return response_data
