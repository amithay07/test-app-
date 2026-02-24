import datetime
import json
import logging
import os
import time
from collections import defaultdict

from django.contrib.sites.shortcuts import get_current_site
from django.db.models import Case
from django.db.models import Count
from django.db.models import F
from django.db.models import FloatField
from django.db.models import Prefetch
from django.db.models import Q
from django.db.models import Sum
from django.db.models import When
from django.db.models.functions import Coalesce
from django.shortcuts import get_object_or_404
from django.utils import timezone
from django.utils.translation import gettext_lazy as _
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework import filters
from rest_framework import status as return_status
from rest_framework import viewsets
from rest_framework.generics import CreateAPIView
from rest_framework.generics import DestroyAPIView
from rest_framework.generics import GenericAPIView
from rest_framework.generics import ListAPIView
from rest_framework.generics import ListCreateAPIView
from rest_framework.parsers import MultiPartParser
from rest_framework.permissions import IsAuthenticated
from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework.response import Response

from jobs.apis.serializers import CloseJobBillSerializer
from jobs.apis.serializers import CloseJobBillUpdateSerializers
from jobs.apis.serializers import CustomJobSerializer
from jobs.apis.serializers import EditJobSerializer
from jobs.apis.serializers import GetTransferJobSerializers
from jobs.apis.serializers import GroupByJobSerializer
from jobs.apis.serializers import JobCreationSerializers
from jobs.apis.serializers import JobTransferSerializer
from jobs.apis.serializers import NotificationSerializer
from jobs.apis.serializers import RecentSearchJobCreateSerializer
from jobs.apis.serializers import ReturnJobListSerializer
from jobs.apis.serializers import ReturnJobSerializer
from jobs.apis.serializers import TransferJobSerializers
from jobs.apis.serializers import UpdateCustomJobSerializer
from jobs.enum import SortBy
from jobs.utils import push_notification
from users.models.bill import TypeCounting
from users.models.group import Group
from users.models.job import CloseJobBill
from users.models.job import Job
from users.models.job import JobAttachment
from users.models.job import JobBill
from users.models.job import JobImage
from users.models.job import JobLog
from users.models.job import JobNote
from users.models.job import JobStatus
from users.models.job import RecentSearchJob
from users.models.job import ReturnJob
from users.models.job import TransferJob
from users.models.notification import Notification
from users.models.role import *
from users.models.user import User
from users.permission import CheckPermission
from users.permission import IsGroupManager
from users.permission import IsInspector
from users.permission import IsSuperUser
from users.permission import UserPermission


logger = logging.getLogger(__name__)


def PushNotification(
    user, address, body, job_id, notification_job_status, notification_type, users
):
    if notification_type in [
        "Transfer",
        "Return",
        "Further_Inspection",
        "Open",
        "priority",
        "Delete",
        "Close",
        "Partial Close",
    ]:
        receivers = users
    else:
        receivers = (
            User.objects.exclude(is_deleted=True)
            .filter(role__title=UserRoleChoices.ADMIN.value)
            .exclude(id=user.id)
        )

    bulk_list = []
    for receiver in receivers:
        notification_data = {}
        notification_data["sender_id"] = user.id
        notification_data["title"] = address
        notification_data["body"] = body
        notification_data["receiver_id"] = receiver.id
        notification_data["created_by"] = user.id
        notification_data["job_id"] = job_id
        notification_data["status"] = notification_job_status
        notification_data["notification_type"] = notification_type
        push_notification(notification_data=notification_data, user=[receiver.id])
        print(receiver.id)
        bulk_list.append(
            Notification(
                sender_id=notification_data["sender_id"],
                receiver_id=notification_data["receiver_id"],
                message=notification_data["body"],
                updated_by_id=user.id,
                created_by_id=notification_data["created_by"],
                job_id=notification_data["job_id"],
                notification_type=notification_data["notification_type"],
            )
        )
    Notification.objects.bulk_create(bulk_list)


class JobCreateView(viewsets.ModelViewSet):
    queryset = Job.objects.exclude(
        id__in=TransferJob.objects.filter(
            group__is_archive=True, is_active=False
        ).values_list("job_id", flat=True)
    )
    serializer_class = JobCreationSerializers
    parser_classes = [MultiPartParser]
    filter_backends = [filters.SearchFilter]
    search_fields = [
        "job__address",
        "job__job_id",
        "job__duplicate_reference",
        "job__address_information",
    ]

    view_permissions = {
        "list": {
            "inspector": True,
            "admin": True,
            "group_manger": True,
            "simple_user": True,
        },
        "create": {
            "inspector": True,
            "admin": True,
            "group_manger": True,
        },
        "retrieve": {
            "inspector": True,
            "admin": True,
            "group_manger": True,
            "simple_user": True,
        },
        "partial_update": {
            "admin": True,
            "inspector": True,
            "group_manger": True,
            "simple_user": True,
        },
    }

    deleted_image = openapi.Parameter(
        "deleted_image",
        openapi.IN_QUERY,
        required=False,
        description="",
        type=openapi.TYPE_STRING,
    )
    deleted_attachment = openapi.Parameter(
        "deleted_attachment",
        openapi.IN_QUERY,
        required=False,
        type=openapi.TYPE_STRING,
    )
    sort_by = openapi.Parameter(
        "sort_by",
        openapi.IN_QUERY,
        required=False,
        type=openapi.TYPE_STRING,
    )

    def get_serializer(self, *args, **kwargs):
        if self.action == "partial_update":
            """Admin and Group Manager can close the job"""
            serializer_class = UpdateCustomJobSerializer
        elif self.action == "create":
            serializer_class = CustomJobSerializer
        else:
            serializer_class = self.serializer_class
        return serializer_class(*args, **kwargs, context={"request": self.request})

    def create(self, request, *args, **kwargs):
        """Admin and inspector can create and get job"""
        data = request.data
        if data:
            data._mutable = True
            data.update({"status": JobStatus.OPEN.value})
        forms = request.data.getlist("form")
        bills = request.data.getlist("bill")
        images = request.data.getlist("image")
        attachment = request.data.getlist("attachment")
        notes = request.data.getlist("notes")
        serializer = self.serializer_class(
            data=data,
            context={
                "request": request,
                "images": images,
                "attachments": attachment,
                "forms": forms,
                "bills": bills,
                "notes": notes,
            },
        )
        serializer.is_valid(raise_exception=True)
        job = serializer.save()

        # Create Transfer Job
        request.data["job"] = job.id
        transfer_job = JobTransferView(
            job_status=JobStatus.OPEN.value,
            is_parent_group=True,
            permission=self.view_permissions["create"],
            further_inspection=job.further_inspection,
            further_billing=job.further_billing,
            is_lock_closed=job.is_lock_closed,
            is_active=True,
        )
        transfer_job = transfer_job.post(request)
        if transfer_job.status_code != 200:
            job.delete()
            return Response(
                transfer_job.data,
                status=return_status.HTTP_400_BAD_REQUEST,
            )
        if request.user.user_name == None or request.user.user_name == "":
            user_by_email = request.user.email.partition("@")
            user_name = user_by_email[0]
        else:
            user_name = request.user.user_name

        
        job_log = JobLog.objects.create(
            job=job,
            created_by=request.user,
            status="Create",
            created_at=timezone.now()
        )

        if str(request.data.get("priority")) == "true":
            notification_job_status = JobStatus.OPEN.value
            notification_type = "Open"
            body = f"המשימה נפתחה על ידי @{user_name}"
            PushNotification(
                request.user,
                request.data["address"],
                body,
                transfer_job.data["tranferd_job_id"],
                notification_job_status,
                notification_type,
                Group.objects.get(id=request.data["group"])
                .member.filter(role_id=3)
                .exclude(id=request.user.id),
            )
        return Response(
            GetTransferJobSerializers(
                TransferJob.objects.get(id=transfer_job.data["tranferd_job_id"]),
                context={"request": self.request},
            ).data,
            status=return_status.HTTP_201_CREATED,
        )

    @swagger_auto_schema(manual_parameters=[sort_by])
    def list(self, request, *args, **kwargs):
        """Inspector can view their job history and serarch their job"""
        sort_by = self.request.query_params.get("sort_by", None)
        order = "-created_at"
        if sort_by == SortBy.ASCENDING:
            order = "created_at"

        instance = self.paginate_queryset(
            self.filter_queryset(
                TransferJob.objects.filter(is_parent_group=True)
                .exclude(
                    Q(created_at__date=datetime.datetime.today())
                    | Q(group__is_archive=True)
                )
                .order_by(order)
                if not request.user.is_superuser
                else TransferJob.objects.filter(
                    is_parent_group=True, job__created_by=request.user.id
                )
                .exclude(
                    Q(created_at__date=datetime.datetime.today())
                    | Q(group__is_archive=True)
                )
                .order_by(order)
            )
        )
        serializer = GetTransferJobSerializers(
            instance, many=True, context={"request": request}
        )
        return self.get_paginated_response(serializer.data)

    @swagger_auto_schema(manual_parameters=[deleted_image, deleted_attachment])
    def partial_update(self, request, *args, **kwargs):
        pk = kwargs.get("pk")
        deleted_image = self.request.query_params.get("deleted_image", None)
        deleted_attachment = self.request.query_params.get("deleted_attachment", None)
        main_group_name = request.data.get("main_group")

        instance = TransferJob.objects.filter(id=pk).first()
        duplicate_not_approved = request.data.get("duplicate_not_approved")
        if not instance:
            # {"detail": "Not found"}
            return Response(
                {"detail": "לא נמצא"},
                status=return_status.HTTP_400_BAD_REQUEST,
            )

        job_id = request.data.get("job_id")

        check_duplicate_job_id = Job.objects.filter(job_id=job_id)
        if check_duplicate_job_id:
            return Response(
                {"job_id": "כבר קיימת משימה עם מזהה זה"},
                status=return_status.HTTP_404_NOT_FOUND,
            )

        if (
            request.data["status"] == JobStatus.CLOSE.value
            and request.data.get("close-update") != "true"
        ):
            if instance.status == JobStatus.CLOSE.value:
                # {"detail": "This Job already closed."}
                return Response(
                    {"detail": "המשימה הזו כבר סגורה"},
                    status=return_status.HTTP_400_BAD_REQUEST,
                )
        # else:
        #     if str(self.request.user.role) in [
        #         UserRoleChoices.GROUP_MANAGER.value,
        #     ]:
        #         # {"detail": "Only admin have permission to update the job"}
        #         return Response(
        #             {"detail": "Only Admin have permission to update the job"},
        #             status=return_status.HTTP_400_BAD_REQUEST,
        #         )
        if not duplicate_not_approved:
            current_user = self.request.user
            if instance.is_parent_group:
                instance.job.status = request.data.get("status")
                instance.job.closed_by = current_user

            if request.data.getlist("form") or request.data.getlist("bill"):
                job_bill, created = JobBill.objects.get_or_create(job=instance)

            if request.data.getlist("form"):
                form_id_list = [int(x) for x in request.data.getlist("form")]
                job_bill.form.add(*form_id_list)
                instance.job.form.add(*form_id_list)

            if request.data.getlist("bill"):
                bill_id_list = [int(x) for x in request.data.getlist("bill")]
                job_bill.bill.add(*bill_id_list)
                instance.job.bill.add(*bill_id_list)

            update_job = (
                request.data["status"]
                in [
                    JobStatus.OPEN.value,
                    JobStatus.TRANSFER.value,
                    JobStatus.RETURN.value,
                    JobStatus.DUPLICATE.value,
                    JobStatus.WRONG_INFORMATION.value,
                ]
                or request.data.get("close-update") == "false"
                or request.data.get("partial-update") == "true"
            )

            if update_job:
                if request.data.get("job_id"):
                    instance.job.job_id = request.data.get("job_id")
                instance.job.address = request.data.get("address")
                instance.job.address_information = request.data.get(
                    "address_information"
                )
                instance.job.description = request.data.get("description")
                instance.job.latitude = request.data.get("latitude")
                instance.job.longitude = request.data.get("longitude")
                instance.group_id = request.data.get("group")
                instance.closed_by = current_user

                if request.data.get("priority"):
                    instance.job.priority = (
                        True if request.data.get("priority") == "true" else False
                    )
                if request.data.get("further_inspection"):
                    instance.job.further_inspection = (
                        True
                        if request.data.get("further_inspection") == "true"
                        else False
                    )
            if request.data.get("job_id"):
                instance.job.job_id = request.data.get("job_id")
            if request.data.get("address"):
                instance.job.address = request.data.get("address")
            if request.data.get("latitude"):
                instance.job.latitude = request.data.get("latitude")
            if request.data.get("longitude"):
                instance.job.longitude = request.data.get("longitude")
            if request.data.get("address_information"):
                instance.job.address_information = request.data.get(
                    "address_information"
                )
            if request.data.get("description"):
                instance.job.description = request.data.get("description")
            if request.data.get("further_inspection"):
                instance.further_inspection, instance.further_billing = (
                    str(request.data.get("further_inspection", False)).capitalize(),
                    str(request.data.get("further_billing", False)).capitalize(),
                )
            instance.updated_by = current_user
            images = request.data.getlist("image")
            attachments = request.data.getlist("attachment")
            created_by = request.user
            updated_by = request.user

            instance.job.further_billing = str(
                request.data.get("further_inspection", False)
            ).capitalize()
            instance.job.updated_by = current_user
            if (
                request.data["status"] not in [JobStatus.CLOSE.value, JobStatus.PARTIAL.value]):

                job_log = JobLog.objects.create(
                job=instance.job,
                updated_by=current_user,
                status="Update",
                created_at=timezone.now()
                )
            

            if (
                request.data["status"]
                in [JobStatus.CLOSE.value, JobStatus.PARTIAL.value]
                or request.data.get("close-update") == "true"
                or request.data.get("partial-update") == "true"
            ):
                instance.job.closed_by = current_user
                if request.data["status"] == JobStatus.CLOSE.value:
                    job_log = JobLog.objects.create(
                        job=instance.job,
                        closed_by=current_user,
                        status="Close",
                        created_at=timezone.now()
                        )
                else:
                    job_log = JobLog.objects.create(
                        job=instance.job,
                        partially_closed_by=current_user,
                        status="Partial",
                        created_at=timezone.now()
                        )

            instance.job.save()
            instance.status = request.data.get("status")

            notes = request.data.getlist("notes")
            for note in notes:
                JobNote.objects.create(
                    job=instance.job,
                    note=note,
                    created_by=current_user
                )
            
            updated_notes  = request.data.get("updated_notes", {})
            updated_notes_dict = json.loads(updated_notes)
            for key, value in updated_notes_dict.items():
                id = int(key)
                note = value
                job_note = JobNote.objects.filter(id=id).first()
                job_note.note = note
                job_note.created_by = current_user
                job_note.save()
                
            for image in images:
                if (
                    request.data["status"] == JobStatus.CLOSE.value
                    or request.data.get("status") == JobStatus.PARTIAL.value
                ):
                    close_job_image = True
                else:
                    close_job_image = False
                JobImage.objects.create(
                    job=instance.job,
                    image=image,
                    created_by=created_by,
                    updated_by=updated_by,
                    close_job_image=close_job_image,
                )

            for attachment in attachments:
                if (
                    request.data["status"] == JobStatus.CLOSE.value
                    or request.data.get("status") == JobStatus.PARTIAL.value
                ):
                    close_job_attachment = True
                else:
                    close_job_attachment = False

                JobAttachment.objects.create(
                    job=instance.job,
                    attachment=attachment,
                    created_by=created_by,
                    updated_by=updated_by,
                    close_job_attachment=close_job_attachment,
                )

            instance.save()
            instance.job.save()
        else:
            instance.job.status = request.data.get("status")
            instance.job.save()
            instance.status = request.data.get("status")
            instance.save()

        parent_group_job = TransferJob.objects.filter(
            job_id=instance.job_id, is_parent_group=True
        ).first()

        if request.data.get("status") == JobStatus.CLOSE.value:
            TransferJob.objects.filter(job_id=instance.job_id).update(
                status=JobStatus.CLOSE.value, is_active=False
            )
            parent_group_job.status = JobStatus.CLOSE.value
            parent_group_job.is_active = True
            parent_group_job.save()
            parent_group_job.job.status = JobStatus.CLOSE.value
            parent_group_job.job.closed_at = timezone.now()
            parent_group_job.job.save()            
        else:
            TransferJob.objects.filter(job_id=instance.job_id, is_active=False).update(
                status=JobStatus.PARTIAL.value
            )

        delete_instance = ReturnJob.objects.filter(
            Q(job__id=pk) | Q(duplicate__id=pk)
        ).first()
        if delete_instance:
            delete_instance.delete()

        if request.user.user_name == None or request.user.user_name == "":
            user_by_email = request.user.email.partition("@")
            user_name = user_by_email[0]
        else:
            user_name = request.user.user_name

        if main_group_name:
            main_group_id = int(main_group_name)
            main_group_job = TransferJob.objects.get(
                job_id=instance.job_id, is_parent_group=True
            )
            job_all_groups = TransferJob.objects.filter(
                job_id=instance.job_id
            ).values_list("group_id", flat=True)

            if main_group_id in job_all_groups:
                main_group_job.is_parent_group = False
                main_group_job.save()
                TransferJob.objects.filter(
                    job_id=instance.job_id, group_id=main_group_id
                ).update(is_parent_group=True)
            else:
                main_group_job.group_id = int(main_group_id)
                main_group_job.save()

        delete_attachment(deleted_image, deleted_attachment)

        if (
            str(request.data.get("further_inspection")) == "true"
            and request.data.get("status") == JobStatus.PARTIAL.value
            or request.data.get("status") == JobStatus.CLOSE.value
        ):
            notification_job_status = (
                JobStatus.CLOSE.value
                if request.data.get("status") == JobStatus.CLOSE.value
                else JobStatus.PARTIAL.value
            )
            notification_type = (
                "Close"
                if request.data.get("status") == JobStatus.CLOSE.value
                else "Partial Close"
            )
            body = f"משימה זו נסגרה על ידי @{user_name}"
            PushNotification(
                request.user,
                parent_group_job.job.address,
                body,
                pk,
                notification_job_status,
                notification_type,
                parent_group_job.group.member.filter(role_id__in=[1, 2]).exclude(
                    id=request.user.id
                ),
            )

        return Response(
            TransferJobSerializers(
                instance,
                context={"request": self.request},
            ).data,
            status=return_status.HTTP_201_CREATED,
        )

    def retrieve(self, request, *args, **kwargs):
        lookup_url_kwarg = self.lookup_url_kwarg or self.lookup_field
        transfer_job = TransferJob.objects.filter(
            id=self.kwargs[lookup_url_kwarg]
        ).first()

        if not transfer_job:
            return Response(
                # {"detail": f"The job was not found with the id {self.kwargs[lookup_url_kwarg]}"}
                {"detail": f"משימה עם מזהה {self.kwargs[lookup_url_kwarg]} לא נמצאה "},
                status=return_status.HTTP_404_NOT_FOUND,
            )
        return Response(
            TransferJobSerializers(
                TransferJob.objects.get(id=transfer_job.id),
                context={"request": self.request},
            ).data,
            status=return_status.HTTP_200_OK,
        )


def delete_attachment(deleted_image, deleted_attachment):
    if deleted_attachment:
        docs_id_list = [int(id) for id in deleted_attachment.split(",")]
        JobAttachment.objects.filter(id__in=docs_id_list).delete()

    if deleted_image:
        image_id_list = [int(id) for id in deleted_image.split(",")]
        JobImage.objects.filter(id__in=image_id_list).delete()


class RecentTransferJob(ListAPIView):
    """
    Group-admin can get recent Transfer jobs
    """

    queryset = TransferJob.objects.exclude(group__is_archive=True)
    serializer_class = JobTransferSerializer
    filter_backends = [filters.SearchFilter]
    search_fields = [
        "job__address",
        "job__job_id",
        "job__duplicate_reference",
        "job__address_information",
    ]
    permission_classes = [IsGroupManager]

    def list(self, request, *args, **kwargs):
        instance = self.paginate_queryset(
            self.filter_queryset(
                self.queryset.filter(
                    is_active=True,
                    created_at__date=datetime.datetime.today(),
                    group__member=request.user.id,
                    is_parent_group=False,
                ).order_by("-created_at")
            )
        )
        serializer = self.serializer_class(
            instance, many=True, context={"request": request}
        )
        return self.get_paginated_response(serializer.data)


class JobTransferView(ListCreateAPIView):
    """Admin can transfer job to one group
    Group admin, manager and inspector can view transfer job
    """

    queryset = TransferJob.objects.filter(is_active=True).exclude(
        group__is_archive=True
    )
    serializer_class = JobTransferSerializer
    filter_backends = [filters.SearchFilter]
    search_fields = [
        "job__address",
        "job__job_id",
        "job__duplicate_reference",
        "job__address_information",
    ]
    permission_classes = [IsAuthenticated]

    def __init__(
        self,
        job_status=None,
        is_parent_group=False,
        permission=None,
        further_inspection=None,
        further_billing=None,
        is_lock_closed=None,
        is_active=True,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if permission:
            self.view_permissions = {
                "get": {"group_manger": True, "admin": True},
                "post": permission,
            }
        else:
            self.view_permissions = {
                "get": {"group_manger": True, "admin": True},
                "post": {"admin": True, "group_manger": True},
            }
        self.permission_classes += [CheckPermission]
        self.further_inspection = further_inspection
        self.is_lock_closed = is_lock_closed
        self.further_billing = further_billing
        self.job_status = job_status
        self.is_parent_group = is_parent_group
        self.is_active = is_active

    def post(self, request, *args, **kwargs):
        group = request.data["group"]
        group_instance = Group.objects.filter(id=group, is_archive=False).first()
        queryset = group_instance.member.filter(
            Q(role__title=UserRoleChoices.GROUP_MANAGER.value)
            | Q(role__title=UserRoleChoices.ADMIN.value),
        ).exclude(id=request.user.id)

        # if str(request.user.role.title) != str(UserRoleChoices.GROUP_MANAGER.value):
        #     queryset = group_instance.member.filter(
        #         role__title=UserRoleChoices.GROUP_MANAGER.value
        #     ).exclude(id=request.user.id)
        #     if not queryset:
        #         # {"detail": "There is no group manager in this group, please add a group manager."}
        #         return Response(
        #             {"detail": "אין מנהל ביצוע לקבוצה זו אנא הוסף מנהל ביצוע"},
        #             status=return_status.HTTP_404_NOT_FOUND,
        #         )
        # else:
        #     queryset = group_instance.member.filter(
        #         role__title=UserRoleChoices.GROUP_MANAGER.value
        #     )

        job = request.data["job"]
        job_detail = Job.objects.filter(id=job).first()
        tranferd_job = TransferJob.objects.filter(group=group, job=job).first()
        if tranferd_job:
            job_transferred_groups = TransferJob.objects.filter(job=job)
            bulk_updated_fields = []
            for job_transferred in job_transferred_groups:
                job_transferred.status = JobStatus.TRANSFER.value
                job_transferred.is_active = False
                bulk_updated_fields.append(job_transferred)

            TransferJob.objects.bulk_update(
                bulk_updated_fields, ["status", "is_active"]
            )
            TransferJob.objects.filter(group=group, job=job).update(
                status=JobStatus.OPEN.value, is_active=True
            )

            job_log = JobLog.objects.create(
            job=job_detail,
            transferred_by=request.user,
            status="Transfer",
            created_at=timezone.now()
             )
            return Response(status=return_status.HTTP_200_OK)

        if job_detail:
            job_detail.status = (
                self.job_status if self.job_status else JobStatus.TRANSFER.value
            )
            job_detail.save()

        serializer = self.serializer_class(
            data=request.data, context={"request": request}
        )
        serializer.is_valid(raise_exception=True)
        TransferJob.objects.filter(job=job, is_active=True).update(is_active=False)
        if self.job_status:
            serializer.validated_data["status"] = self.job_status
        else:
            serializer.validated_data["status"] = JobStatus.OPEN.value
            tranferd_job_obj = TransferJob.objects.filter(
                is_parent_group=True, job=job
            ).first()
            tranferd_job_obj.save()
        if self.is_parent_group:
            serializer.validated_data["is_parent_group"] = True
        if self.further_billing:
            serializer.validated_data["further_billing"] = self.further_billing
        if self.further_inspection:
            serializer.validated_data["further_inspection"] = self.further_inspection
        if self.is_lock_closed:
            serializer.validated_data["is_lock_closed"] = self.is_lock_closed
        if self.is_active:
            serializer.validated_data["is_active"] = self.is_active
        tranferd_job = serializer.save()
        tranferd_job.further_inspection = job_detail.further_inspection
        tranferd_job.save()
        if not request.user.user_name:
            user_by_email = request.user.email.partition("@")
            user_name = user_by_email[0]
        else:
            user_name = request.user.user_name
        body = f"משימה זו הועברה על ידי @{user_name}"
        notification_job_status = JobStatus.TRANSFER.value
        notification_type = "Transfer"
        PushNotification(
            request.user,
            job_detail.address,
            body,
            tranferd_job.id,
            notification_job_status,
            notification_type,
            queryset,
        )
        response = serializer.data
        response["tranferd_job_id"] = tranferd_job.id
        return Response(response)

    def get(self, request, *args, **kwargs):
        """Group manager can view transfer job"""
        instance = self.paginate_queryset(
            self.filter_queryset(
                self.queryset.filter(
                    group__member=request.user.id,
                    is_parent_group=False,
                )
                .exclude(
                    created_at__date=datetime.datetime.today(),
                )
                .order_by("-created_at")
            )
        )
        serializer = self.serializer_class(
            instance=instance, many=True, context={"request": request}
        )
        return self.get_paginated_response(serializer.data)


def is_valid_date(date_string):
    try:
        datetime.datetime.strptime(date_string, "%Y-%m-%d")
        return True
    except ValueError:
        return False


class GroupJobView(GenericAPIView):
    """
    Admin/Inspector/Group manager can view job group wise.
    Add status as a query parameter and it is compulsory.
    """

    queryset = TransferJob.objects.exclude(group__is_archive=True)

    serializer_class = JobCreationSerializers
    filter_backends = [filters.SearchFilter]
    search_fields = [
        "job__address",
        "job__job_id",
        "job__duplicate_reference",
        "job__address_information",
    ]
    permission_classes = [IsAuthenticated]

    status = openapi.Parameter(
        "status",
        openapi.IN_QUERY,
        required=False,
        description="Status should be Open or Close in Hebrew",
        type=openapi.TYPE_STRING,
    )

    module = openapi.Parameter(
        "module",
        openapi.IN_QUERY,
        required=False,
        description="Status should be duplicate-jobs or map or jobs",
        type=openapi.TYPE_STRING,
    )

    id = openapi.Parameter(
        "id",
        openapi.IN_QUERY,
        required=False,
        description="Enter Group Id",
        type=openapi.TYPE_STRING,
    )
    from_date = openapi.Parameter(
        "from_date",
        openapi.IN_QUERY,
        required=False,
        type=openapi.TYPE_STRING,
    )
    to_date = openapi.Parameter(
        "to_date",
        openapi.IN_QUERY,
        required=False,
        type=openapi.TYPE_STRING,
    )
    sort_by = openapi.Parameter(
        "sort_by",
        openapi.IN_QUERY,
        required=False,
        type=openapi.TYPE_STRING,
    )

    @swagger_auto_schema(manual_parameters=[status, module, id, from_date, to_date, sort_by])
    def get(self, request, *args, **kwargs):
        status = self.request.query_params.get("status", None)
        module = self.request.query_params.get("module", None)
        group_id = self.request.query_params.get("id", None)
        from_date = self.request.query_params.get("from_date", None)
        to_date = self.request.query_params.get("to_date", None)
        sort_by = self.request.query_params.get("sort_by", None)

        if from_date:
            if is_valid_date(from_date) == False:
                return Response(
                    {"detail": _("Something went wrong")},
                    status=return_status.HTTP_422_UNPROCESSABLE_ENTITY,
                )

        if to_date:
            if is_valid_date(to_date) == False:
                return Response(
                    {"detail":_("Something went wrong")},
                    status=return_status.HTTP_422_UNPROCESSABLE_ENTITY,
                )

        job_status = (
            [JobStatus.OPEN.value, JobStatus.TRANSFER.value]
            if module == "map"
            else [
                JobStatus.OPEN.value,
                JobStatus.TRANSFER.value,
                JobStatus.PARTIAL.value,
                JobStatus.CLOSE.value,
                JobStatus.RETURN.value,
            ]
            if module == "duplicate-jobs"
            else [
                JobStatus.OPEN.value,
                JobStatus.TRANSFER.value,
                JobStatus.PARTIAL.value,
            ]
        )

        if status:
            status = status.capitalize()
        if request.user.is_superuser:
            condition = Q(id=group_id)
            condition_of_all_groups = Q(group__is_archive=False)
        else:
            condition = Q(id=group_id, member=request.user.id)
            condition_of_all_groups = Q(
                group__is_archive=False, group__member=request.user
            )
        if status == "סגור":
            order = "-job__closed_at"
            if sort_by == SortBy.ASCENDING:
                order = "job__closed_at"
        else:
            order = "-created_at"
            if sort_by == SortBy.ASCENDING:
                order = "created_at"

        if group_id:  # Group_id
            group = Group.objects.filter(condition).exclude(is_archive=True).first()
            instance = self.filter_queryset(
                TransferJob.objects.filter(
                    group=group,
                    status__in=job_status
                    if status == JobStatus.OPEN.value
                    else [status],
                )
                .exclude(group__is_archive=True)
                .order_by(order)
            )
        else:
            instance = self.filter_queryset(
                TransferJob.objects.filter(
                    condition_of_all_groups,
                    status__in=job_status
                    if status == JobStatus.OPEN.value
                    else [status],
                )
                .exclude(group__is_archive=True)
                .order_by(order)
            )

        if from_date and to_date:
            if status == JobStatus.OPEN.value:
                instance = instance.filter(
                    Q(created_at__date__gte=from_date)
                    & Q(created_at__date__lte=to_date)
                ).annotate(Count("id"))
            elif status == JobStatus.CLOSE.value:
                instance = instance.filter(
                    Q(job__closed_at__date__gte=from_date)
                    & Q(job__closed_at__date__lte=to_date)
                ).annotate(Count("id"))


        elif from_date:
            if status == JobStatus.OPEN.value:
                instance = instance.filter(created_at__date=from_date)
            elif status == JobStatus.CLOSE.value:
                instance = instance.filter(job__closed_at__date=from_date)

        serializer = GroupByJobSerializer(
            self.paginate_queryset(instance),
            many=True,
            context={"request": request},
        )
        return self.get_paginated_response(serializer.data)


class MapJobView(GenericAPIView):
    """
    Admin/Inspector/Group manager can view job group wise.
    Add status as a query parameter and it is compulsory.
    """

    queryset = TransferJob.objects.exclude(group__is_archive=True, is_active=False)

    serializer_class = JobCreationSerializers
    filter_backends = [filters.SearchFilter]
    search_fields = [
        "job__address",
        "job__job_id",
        "job__duplicate_reference",
        "job__address_information",
    ]
    permission_classes = [IsAuthenticated]

    id = openapi.Parameter(
        "id",
        openapi.IN_QUERY,
        required=False,
        description="Enter Group Id",
        type=openapi.TYPE_STRING,
    )
    sort_by = openapi.Parameter(
        "sort_by",
        openapi.IN_QUERY,
        required=False,
        type=openapi.TYPE_STRING,
    )

    @swagger_auto_schema(manual_parameters=[id, sort_by])
    def get(self, request, *args, **kwargs):
        group_id = self.request.query_params.get("id", None)
        sort_by = self.request.query_params.get("sort_by", None)
        order = "-created_at"
        if sort_by == SortBy.ASCENDING:
            order = "created_at"
        
        if request.user.is_superuser:
            condition = Q(id=group_id)
            condition_of_all_groups = Q(group__is_archive=False)
        else:
            condition = Q(id=group_id, member=request.user.id)
            condition_of_all_groups = Q(
                group__is_archive=False, group__member=request.user
            )

        if group_id:  # Group_id
            group = Group.objects.filter(condition).exclude(is_archive=True).first()
            instance = self.filter_queryset(
                TransferJob.objects.filter(
                    group=group,
                    status__in=[JobStatus.OPEN.value, JobStatus.TRANSFER.value],
                    is_active=True,
                )
                .exclude(group__is_archive=True)
                .order_by(order)
            )
        else:
            instance = self.filter_queryset(
                TransferJob.objects.filter(
                    condition_of_all_groups,
                    status__in=[JobStatus.OPEN.value, JobStatus.TRANSFER.value],
                    is_active=True,
                )
                .exclude(group__is_archive=True)
                .order_by(order)
            )

        serializer = GroupByJobSerializer(
            instance=instance, many=True, context={"request": request}
        )
        return Response({"results": serializer.data})


class RecentAddJobView(ListAPIView):
    queryset = Job.objects.all()
    serializer_class = JobCreationSerializers
    filter_backends = [filters.SearchFilter]
    search_fields = [
        "job__address",
        "job__job_id",
        "job__duplicate_reference",
        "job__address_information",
    ]
    permission_classes = [UserPermission | IsInspector]

    def list(self, request, *args, **kwargs):
        """Inspector and Admin can view their recently created job"""
        instance = self.paginate_queryset(
            self.filter_queryset(
                TransferJob.objects.filter(
                    created_by=request.user.id,
                    created_at__date=datetime.datetime.today(),
                    is_active=True,
                )
                .exclude(group__is_archive=True)
                .order_by("-created_at")
            )
        )
        serializer = GetTransferJobSerializers(
            instance, many=True, context={"request": request}
        )
        return self.get_paginated_response(serializer.data)


class ReturnJobView(viewsets.ModelViewSet):
    """
    Admin and Group-Manager can return job
    """

    queryset = ReturnJob.objects.exclude(
        Q(created_at__date=datetime.datetime.today()) | Q(group__is_archive=True),
    )
    serializer_class = ReturnJobSerializer
    filter_backends = [filters.SearchFilter]
    search_fields = [
        "job__job__address",
        "job__job__job_id",
        "job__job__address_information",
    ]

    view_permissions = {
        "list": {"admin": True, "inspector": True},
        "create": {"admin": True, "group_manger": True},
    }

    def create(self, request, *args, **kwargs):
        data = request.data

        job = TransferJob.objects.filter(id=data["job"], is_active=True).first()

        parent_group = TransferJob.objects.filter(
            job_id=job.job_id, is_parent_group=True
        ).first()

        if not job:
            # {"detail": "Job transfer required."}
            return Response(
                {"detail": "יש להעביר את המשימה"},
                status=return_status.HTTP_404_NOT_FOUND,
            )
        if "duplicate" in data:
            dublicate_instance = TransferJob.objects.filter(id=data["job"]).first()
            if not dublicate_instance:
                # {"detail": "Job transfer required."}
                return Response(
                    {"detail": "יש להעביר את המשימה"},
                    status=return_status.HTTP_404_NOT_FOUND,
                )

            if dublicate_instance.status in [
                JobStatus.CLOSE.value,
                JobStatus.RETURN.value,
            ]:
                # You are unable to {JobStatus.RETURN.value} this job as your selected duplicate job is currently in the {dublicate_instance.status} phase.
                return Response(
                    {
                        "detail": f"אינך יכול לבצע {JobStatus.RETURN.value} משרה זו מכיוון שהמשרה הכפולה שבחרת נמצאת כעת בשלב {dublicate_instance.status}."
                    },
                    status=return_status.HTTP_400_BAD_REQUEST,
                )
        else:
            if job.status in [
                JobStatus.CLOSE.value,
                JobStatus.RETURN.value,
            ]:
                # {"detail": f"You are unable to {JobStatus.RETURN.value} this job as it is currently in the {job.status} phase."}
                return Response(
                    {
                        "detail": f"אינך יכול {JobStatus.RETURN.value} עבודה זו מכיוון שהיא נמצאת כעת בשלב {job.status}."
                    },
                    status=return_status.HTTP_400_BAD_REQUEST,
                )
        # main_job = TransferJob.objects.filter(job=job.job, is_parent_group=True).first()
        main_job_group = parent_group.group

        main_group_user = main_job_group.member.filter(
            role__title__in=[
                UserRoleChoices.INSPECTOR.value,
                UserRoleChoices.ADMIN.value,
            ]
        ).exclude(id=request.user.id)
        return_to = list(main_group_user.values_list("id", flat=True))

        inspector_queryset = main_job_group.member.filter(
            role__title=UserRoleChoices.INSPECTOR.value
        ).exclude(id=request.user.id)

        if "duplicate" in data:
            job = dublicate_instance
            job.closed_by = request.user
            data.update(
                {
                    "return_to": return_to,
                    "job": data["duplicate"],
                    "duplicate": parent_group.id,
                }
            )
            TransferJob.objects.filter(job_id=parent_group.job_id).update(
                is_active=False
            )
            duplicate_job = TransferJob.objects.filter(
                job_id=parent_group.job_id
            ).first()
            duplicate_job.job.status = JobStatus.RETURN.value
            duplicate_job.job.save()

        if "duplicate" not in data:
            job.job.status = JobStatus.RETURN.value
            job.is_active = False
            job.save()
            job.job.save()
            data.update({"return_to": return_to, "job": parent_group.id})

        serializer = self.serializer_class(data=data, context={"request": request})
        serializer.is_valid(raise_exception=True)
        returun_job = serializer.save()

        parent_group.status = JobStatus.RETURN.value
        parent_group.is_active = True
        parent_group.save()

        if not request.user.user_name:
            user_by_email = request.user.email.partition("@")
            user_name = user_by_email[0]
        else:
            user_name = request.user.user_name

        body = f"Job is Returned by @{user_name}"
        notification_job_status = JobStatus.RETURN.value
        notification_type = "Return"

        job_log = JobLog.objects.create(
            job=job.job,
            returned_by=request.user,
            status="Return",
            created_at=timezone.now()
        )

        PushNotification(
            request.user,
            job.job.address,
            body,
            job.id,
            notification_job_status,
            notification_type,
            inspector_queryset,
        )

        return Response(
            ReturnJobListSerializer(returun_job, context={"request": request}).data
        )

    def list(self, request, *args, **kwargs):
        returun_job = self.paginate_queryset(
            self.filter_queryset(
                self.queryset.filter(
                    return_to=request.user.id,
                    status__in=["מידע שגוי", "לְשַׁכְפֵּל"],
                )
                .exclude(created_at__date=datetime.datetime.today())
                .order_by("-created_at")
                if not request.user.is_superuser
                else self.queryset.filter(
                    status__in=["מידע שגוי", "לְשַׁכְפֵּל"],
                )
                .exclude(created_at__date=datetime.datetime.today())
                .order_by("-created_at")
            )
        )
        serializer = ReturnJobListSerializer(
            returun_job, many=True, context={"request": request}
        )
        return self.get_paginated_response(serializer.data)


class ReturnJobUpdateView(viewsets.ModelViewSet):
    queryset = ReturnJob.objects.all()
    serializer_class = ReturnJobSerializer
    parser_classes = [MultiPartParser]

    view_permissions = {
        "retrieve": {"admin": True, "inspector": True},
        "partial_update": {"admin": True, "inspector": True},
        "destroy": {"admin": True, "inspector": True},
    }

    def get_serializer(self, *args, **kwargs):
        if self.action == "partial_update":
            serializer_class = EditJobSerializer
        else:
            serializer_class = self.serializer_class
        return serializer_class(*args, **kwargs, context={"request": self.request})

    def retrieve(self, request, *args, **kwargs):
        pk = kwargs.get("pk")
        return_job = ReturnJob.objects.filter(id=pk).first()
        if not return_job:
            # {"detail": "Return Job not found"}
            return Response(
                {"detail": "משימה שהוחזרה לא נמצאה"},
                status=return_status.HTTP_404_NOT_FOUND,
            )
        serializer = ReturnJobListSerializer(
            instance=return_job, context={"request": request}
        )
        return Response(serializer.data, status=return_status.HTTP_200_OK)

    # TODO chages required base on frontend
    def partial_update(self, request, *args, **kwargs):
        """
        Admin and inspector can update their wrong information return job
        """
        pk = kwargs.get("pk")
        return_job = ReturnJob.objects.filter(id=pk).first()
        if not return_job:
            # {"detail": "Return Job not found"}
            return Response(
                {"detail": "משימה שהוחזרה לא נמצאה"},
                status=return_status.HTTP_404_NOT_FOUND,
            )
        if not return_job.status in ["מידע שגוי", "לְשַׁכְפֵּל"]:
            # {"detail": "Only wrong information jobs and duplicate jobs can be updated"}
            return Response(
                {"detail": "רק משימות עם מידע שגוי או כפילות יכולות להתעדכן"},
                status=return_status.HTTP_400_BAD_REQUEST,
            )

        if return_job.duplicate:
            transfer_job = TransferJob.objects.filter(
                id=return_job.duplicate.id
            ).first()
        else:
            transfer_job = TransferJob.objects.filter(id=return_job.job.id).first()

        job = transfer_job.job
        images = request.data.getlist("image")
        attachments = request.data.getlist("attachment")
        update_serializer = EditJobSerializer(
            job,
            data=request.data,
            context={"request": request, "images": images, "attachments": attachments},
        )
        update_serializer.is_valid(raise_exception=True)
        update_serializer.save()
        transfer_job.status = JobStatus.OPEN.value
        transfer_job.is_active = False
        transfer_job.save()
        return_job.delete()
        transfer_job.job.status = JobStatus.OPEN.value
        TransferJob.objects.filter(
            group_id=return_job.group_id, job_id=transfer_job.job_id
        ).update(is_active=True)

        TransferJob.objects.filter(
            job_id=transfer_job.job_id,
            is_parent_group=False,
            group_id=return_job.group_id,
        ).update(is_active=True)

        serializer = TransferJobSerializers(transfer_job, context={"request": request})

        if not request.user.user_name:
            user_by_email = request.user.email.partition("@")
            user_name = user_by_email[0]
        else:
            user_name = request.user.user_name

        body = f"משימה זו עודכנה על ידי @{user_name}"
        notification_job_status = JobStatus.OPEN.value
        notification_type = "Open"

        job_log = JobLog.objects.create(
            job=job,
            updated_by=request.user,
            status="Update",
            created_at=timezone.now()
        )
        PushNotification(
            request.user,
            transfer_job.job.address,
            body,
            transfer_job.id,
            notification_job_status,
            notification_type,
            TransferJob.objects.get(is_parent_group=True, job=transfer_job.job)
            .group.member.filter(role_id__in=[1, 3])
            .exclude(id=request.user.id),
        )

        return Response(serializer.data, status=return_status.HTTP_202_ACCEPTED)

    def destroy(self, request, *args, **kwargs):
        """
        Admin and inspector can delete their return job
        """
        pk = kwargs.get("pk")
        return_job = ReturnJob.objects.filter(id=pk).first()
        if not return_job:
            return Response(
                # {"detail": "Job not found"}
                {"detail": "עבודה לא נמצאה"},
                status=return_status.HTTP_404_NOT_FOUND,
            )
        if (
            not return_job.status == JobStatus.WRONG_INFORMATION.value
            and not return_job.status == JobStatus.DUPLICATE.value
        ):
            # {"detail": "Only wrong information jobs and duplicate jobs can be deleted"}
            return Response(
                {"detail": "רק משימות כפולות או שגויות ניתנות למחיקה"},
                status=return_status.HTTP_400_BAD_REQUEST,
            )

        if return_job.duplicate:
            transfer_job = TransferJob.objects.filter(
                id=return_job.duplicate.id
            ).first()
        else:
            transfer_job = TransferJob.objects.filter(id=return_job.job.id).first()

        transfer_job_id = transfer_job.job_id
        address = transfer_job.job.address
        main_group_memebr = transfer_job.group.member.filter(
            role_id__in=[1, 3]
        ).exclude(id=request.user.id)
        if not transfer_job:
            # {"detail": "Job not found"}
            return Response(
                {"detail": "עבודה לא נמצאה"}, status=return_status.HTTP_404_NOT_FOUND
            )

        TransferJob.objects.filter(id=transfer_job_id).delete()
        Job.objects.filter(id=transfer_job_id).delete()
        if not request.user.user_name:
            user_by_email = request.user.email.partition("@")
            user_name = user_by_email[0]
        else:
            user_name = request.user.user_name

        body = f"משימה זו נמחקה על ידי @{user_name}"
        notification_job_status = "Delete"
        notification_type = "Delete"
        PushNotification(
            request.user,
            address,
            body,
            None,
            notification_job_status,
            notification_type,
            main_group_memebr,
        )
        # {"detail": "Job deleted successfully"}
        return Response(
            {"detail": "משימה נמחקה בהצלחה"}, status=return_status.HTTP_202_ACCEPTED
        )


class RecentReturnJobView(ListAPIView):
    queryset = ReturnJob.objects.exclude(
        job_id__in=TransferJob.objects.filter(group__is_archive=True).values_list(
            "id", flat=True
        )
    )
    serializer_class = ReturnJobListSerializer
    filter_backends = [filters.SearchFilter]
    search_fields = [
        "job__job__address",
        "job__job__job_id",
        "job__job__address_information",
    ]
    permission_classes = [UserPermission | IsInspector]

    def list(self, request, *args, **kwargs):
        instance = self.paginate_queryset(
            self.filter_queryset(
                self.queryset.filter(
                    return_to=request.user.id,
                    status__in=["מידע שגוי", "לְשַׁכְפֵּל"],
                    created_at__date=datetime.datetime.today(),
                ).order_by("-created_at")
                if not request.user.is_superuser
                else self.queryset.filter(
                    status__in=["מידע שגוי", "לְשַׁכְפֵּל"],
                    created_at__date=datetime.datetime.today(),
                ).order_by("-created_at")
            )
        )
        serializer = self.serializer_class(
            instance, many=True, context={"request": request}
        )
        return self.get_paginated_response(serializer.data)


class JobNotification(ListAPIView):
    queryset = Notification.objects.exclude(job__isnull=True)
    serializer_class = NotificationSerializer
    permission_classes = [IsAuthenticated]

    def list(self, request, *args, **kwargs):
        instance = self.paginate_queryset(
            self.queryset.filter(receiver_id=request.user.id).order_by("-created_at")
        )
        serializer = self.serializer_class(
            instance, many=True, context={"request": request}
        )
        return self.get_paginated_response(serializer.data)


class RecentSearchJobsListCreateView(ListCreateAPIView):
    queryset = RecentSearchJob.objects.all()
    serializer_class = RecentSearchJobCreateSerializer
    permission_classes = [IsAuthenticated]

    def list(self, request, *args, **kwargs):
        recent_search_data = (
            RecentSearchJob.objects.filter(
                created_by=request.user.id,
                job__in=TransferJob.objects.filter(group__is_archive=False).values_list(
                    "id", flat=True
                ),
            )
            .order_by("-created_at")
            .exclude(
                Q(job__status=JobStatus.CLOSE.value)
                | Q(job__job__status=JobStatus.CLOSE.value)
            )
        )
        serializer = self.serializer_class(
            recent_search_data, many=True, context={"request": request}
        )
        return Response(serializer.data)

    def post(self, request, *args, **kwargs):
        current_user = self.request.user
        data = request.data
        duplicate_job = RecentSearchJob.objects.filter(
            job_id=data["job"], created_by=current_user.id
        )
        duplicate_job.delete()
        serializer = self.serializer_class(data=data, context={"request": request})
        serializer.is_valid(raise_exception=True)
        recent_job = serializer.save()
        recent_job.created_by = current_user
        recent_job.save()
        if 15 < RecentSearchJob.objects.filter(created_by=current_user.id).count():
            RecentSearchJob.objects.filter(
                pk__in=RecentSearchJob.objects.filter(created_by=current_user.id)
                .order_by("-created_at")
                .values_list("pk")[15:]
            ).delete()
        return Response(serializer.data)


class ReportGeneratorView(ListAPIView):
    queryset = TransferJob.objects.filter(status=JobStatus.CLOSE.value)
    serializer_class = TransferJobSerializers
    permission_classes = [UserPermission]
    URL = os.environ["URL"]

    report = openapi.Parameter(
        "report",
        openapi.IN_QUERY,
        required=False,
        description="report should be detail or sum_up",
        type=openapi.TYPE_STRING,
    )
    from_date = openapi.Parameter(
        "from_date",
        openapi.IN_QUERY,
        required=False,
        type=openapi.TYPE_STRING,
    )
    to_date = openapi.Parameter(
        "to_date",
        openapi.IN_QUERY,
        required=False,
        type=openapi.TYPE_STRING,
    )
    groups = openapi.Parameter(
        "groups",
        openapi.IN_QUERY,
        type=openapi.TYPE_ARRAY,
        items=openapi.Items(type=openapi.TYPE_INTEGER),
        required=False,
    )

    @swagger_auto_schema(manual_parameters=[from_date, to_date, report, groups])
    def get(self, request, *args, **kwargs):
        report = self.request.query_params.get("report", None)
        from_date = self.request.query_params.get("from_date", None)
        to_date = self.request.query_params.get("to_date", None)
        groups = self.request.query_params.get("groups", None)

        if groups:
            group_list = [int(x) for x in groups.split(",")]

            jobs_by_group = self.queryset.filter(group_id__in=group_list).exclude(
                is_parent_group=False
            )

            job_id_of_main_group = jobs_by_group.filter(
                is_parent_group=True
            ).values_list("job", flat=True)
            child_jobs_of_main_job = TransferJob.objects.filter(
                job_id__in=job_id_of_main_group
            ).values_list("id", flat=True)
            child_jobs_of_group = jobs_by_group.exclude(
                job_id__in=job_id_of_main_group
            ).values_list("id", flat=True)

            tranfer_job_id_list = list(child_jobs_of_main_job) + list(
                child_jobs_of_group
            )
        else:
            tranfer_job_id_list = list(self.queryset.values_list("id", flat=True))

        queryset = self.queryset.filter(id__in=tranfer_job_id_list)
        if from_date and to_date:
            instances = queryset.filter(
                updated_at__date__gte=from_date,
                updated_at__date__lte=to_date,
            )
        elif from_date:
            instances = queryset.filter(updated_at__date__gte=from_date)
        else:
            pass

        if report == "detail":
            close_job_id = []
            for instance in instances:
                close_job_id.append(instance.job_id)

            close_job = TransferJob.objects.filter(
                job_id__in=list(set(close_job_id)), is_parent_group=True
            )
            serializer = self.serializer_class(
                self.paginate_queryset(close_job),
                many=True,
                context={"request": request},
            )
            return self.get_paginated_response(serializer.data)

        elif report == "sum_up":
            instances = instances.values_list("job_id", flat=True)
            bill_data_list = CloseJobBill.objects.filter(job__job_id__in=instances)

            bills = []
            list_of_sign_bills = []
            list_of_material_bills = []

            # append all bills in bills(list)
            for bill in bill_data_list:
                bills.append(
                    {
                        "name": bill.name,
                        "type": bill.type,
                        "type_counting": bill.type_counting,
                        # "image": f"{self.URL}/media/{bill.image}"
                        # if bill.image
                        # else self.request.build_absolute_uri("/")
                        # + "static/assets/img/bill.svg"
                        # if bill.type == "Sign"
                        # else None,
                    },
                )

            unique_sign = []
            unique_material = []
            unique_keys = set()
            for bill in bills:
                if bill["type"] == "Sign":
                    key = (bill["type"], bill["type_counting"])
                    if key not in unique_keys:
                        unique_sign.append(bill)
                        unique_keys.add(key)
                elif bill["type"] == "Material":
                    key = (bill["name"], bill["type_counting"])
                    if key not in unique_keys:
                        unique_material.append(bill)
                        unique_keys.add(key)

            # Count jumping_ration
            for data in unique_sign:
                bill_quantity = bill_data_list.filter(
                    type=data["type"],
                    type_counting=data["type_counting"],
                ).aggregate(
                    QTY=Sum(
                        F("measurement") * Coalesce(F("jumping_ration"), 1),
                        output_field=FloatField(),
                    )
                )

                if data["type_counting"] == TypeCounting.SQM.value:
                    data["name"] = "תמרורים לפי מ״ר (439)"
                if data["type_counting"] == TypeCounting.UNITS.value:
                    data["name"] = "תמרורים"
                data["quantity"] = bill_quantity["QTY"]
                list_of_sign_bills.append(data)

            for data in unique_material:
                bill_quantity = bill_data_list.filter(
                    name=data["name"],
                    type_counting=data["type_counting"],
                ).aggregate(
                    QTY=Sum(
                        F("measurement") * Coalesce(F("jumping_ration"), 1),
                        output_field=FloatField(),
                    )
                )

                data["quantity"] = bill_quantity["QTY"]
                list_of_material_bills.append(data)

            list_of_bills = {
                "material": list_of_material_bills,
                "sign_bill": list_of_sign_bills,
            }

        return Response({"bills": list_of_bills})


class PdfGeneratorView(ListAPIView):
    queryset = TransferJob.objects.filter(
        status=JobStatus.CLOSE.value, is_active=True
    ).select_related("job", "group").prefetch_related(
        Prefetch("job__job_image", queryset=JobImage.objects.order_by("id"))
    )
    serializer_class = JobCreationSerializers
    permission_classes = [UserPermission | IsInspector]
    renderer_classes = [TemplateHTMLRenderer]
    URL = ReportGeneratorView.URL

    report = openapi.Parameter(
        "report",
        openapi.IN_QUERY,
        required=False,
        description="report should be detail or sum_up",
        type=openapi.TYPE_STRING,
    )
    from_date = openapi.Parameter(
        "from_date",
        openapi.IN_QUERY,
        required=False,
        type=openapi.TYPE_STRING,
    )
    to_date = openapi.Parameter(
        "to_date",
        openapi.IN_QUERY,
        required=False,
        type=openapi.TYPE_STRING,
    )
    groups = openapi.Parameter(
        "groups",
        openapi.IN_QUERY,
        type=openapi.TYPE_ARRAY,
        items=openapi.Items(type=openapi.TYPE_INTEGER),
        required=False,
    )

    single_job_id = openapi.Parameter(
        "single_job_id",
        openapi.IN_QUERY,
        type=openapi.TYPE_ARRAY,
        items=openapi.Items(type=openapi.TYPE_INTEGER),
        required=False,
    )

    report_with_image = openapi.Parameter(
        "report_with_image",
        openapi.IN_QUERY,
        type=openapi.TYPE_ARRAY,
        items=openapi.Items(type=openapi.TYPE_INTEGER),
        required=False,
    )

    @swagger_auto_schema(
        manual_parameters=[from_date, to_date, groups, single_job_id, report_with_image]
    )
    def get(self, request, *args, **kwargs):
        start_time = time.time()
        request_id = request.META.get('HTTP_X_REQUEST_ID', 'N/A')
        
        report = self.request.query_params.get("report", None)
        from_date = self.request.query_params.get("from_date", None)
        to_date = self.request.query_params.get("to_date", None)

        from_date_obj = datetime.datetime.strptime(from_date, "%Y-%m-%d")
        to_date_obj = datetime.datetime.strptime(to_date, "%Y-%m-%d")

        date_data = {
            "from_date": from_date_obj.strftime("%d-%m-%Y"),
            "to_date": to_date_obj.strftime("%d-%m-%Y"),
        }
        groups = self.request.query_params.get("groups", None)
        single_job_id = self.request.query_params.get("single_job_id", None)
        single_report = True if single_job_id else False
        report_with_image = self.request.query_params.get("report_with_image", None)
        if groups:
            group_name = Group.objects.get(id=groups)

        if groups:
            group_list = [int(x) for x in groups.split(",")]

            jobs_by_group = self.queryset.filter(group__id__in=group_list)
            job_id_of_main_group = jobs_by_group.filter(
                is_parent_group=True
            ).values_list("job", flat=True)
            child_jobs_of_main_job = TransferJob.objects.filter(
                job_id__in=job_id_of_main_group
            ).values_list("id", flat=True)
            child_jobs_of_group = jobs_by_group.exclude(
                job_id__in=job_id_of_main_group
            ).values_list("id", flat=True)

            tranfer_job_id_list = list(child_jobs_of_main_job) + list(
                child_jobs_of_group
            )

        else:
            tranfer_job_id_list = list(self.queryset.values_list("id", flat=True))
            group_list = list(self.queryset.distinct().values_list("group", flat=True))

        queryset = (
            TransferJob.objects.filter(id=single_job_id).prefetch_related(
                "job__job_image"
            )
            if single_job_id
            else self.queryset.filter(id__in=tranfer_job_id_list)
        )

        if single_job_id:
            instances = queryset
        else:
            if from_date and to_date:
                instances = queryset.filter(
                    job__closed_at__date__gte=from_date,
                    job__closed_at__date__lte=to_date,
                ).order_by("-job__closed_at")
            elif from_date:
                instances = queryset.filter(job__closed_at__date__gte=from_date).order_by("-job__closed_at")
            else:
                instances = queryset.order_by("-job__closed_at")

        query_time = time.time() - start_time
        job_count = instances.count()
        logger.info(f"[{request_id}] PdfGeneratorView report={report}: Query completed in {query_time:.2f}s, {job_count} jobs")

        if report == "detail":
            data = []
            for instance in instances:
                new_dict = {}
                group_data = instance.group
                user_data = instance.group.member.filter(
                    role__title=UserRoleChoices.GROUP_MANAGER.value
                ).values_list("user_name", flat=True)

                new_dict["id"] = instance.job.id
                new_dict["job_id"] = instance.job.job_id
                new_dict["group_name"] = group_data.name
                new_dict["group_manager"] = user_data
                new_dict["address"] = instance.job.address
                new_dict["address_information"] = instance.job.address_information
                if instance.status == JobStatus.CLOSE.value:
                    if instance.job.closed_by:
                        if instance.job.closed_by.user_name:
                            closed_by = instance.job.closed_by.user_name
                            new_dict["close_by"] = closed_by
                        else:
                            closed_by = instance.job.closed_by.email
                            new_dict["close_by"] = closed_by

                    new_dict["notes"] = (
                        instance.job.job_notes.all()
                    )
                    new_dict["description"] = instance.job.description  
                new_dict["closed_at"] = instance.job.closed_at.date() if instance.job.closed_at else instance.job.updated_at.date()

                if single_job_id:
                    new_dict["current_group_name"] = instance.group
                    new_dict["created_by"] = instance.job.created_by
                    new_dict["close_by"] = instance.job.closed_by
                    new_dict["further_inspection"] = instance.job.further_inspection

                if report_with_image == "true":
                    extensions = [
                        "mp4",
                        "m4v",
                        "webm",
                        "ogg",
                        "ogv",
                        "MOV",
                        "AVI",
                        "MKV",
                    ]

                    new_dict["images"] = instance.job.job_image.filter(
                        close_job_image=False
                    ).exclude(image__regex="|".join(extensions))
                    new_dict["close_images"] = instance.job.job_image.filter(
                        close_job_image=True
                    ).exclude(image__regex="|".join(extensions))

                # Query bills per instance - EXACTLY like preview does
                instance_bills = CloseJobBill.objects.filter(job__job_id=instance.job_id)
                logger.info(f"[{request_id}] PdfGenerator - Job {instance.job_id}: Found {instance_bills.count()} bills")
                
                sign_bills_list = []
                detail_bills_list = []

                for bill in instance_bills:
                    print(f"DEBUG PdfGenerator - Job {instance.job_id}: Bill {bill.name}, type={bill.type}, measurement={bill.measurement}", flush=True)
                    if bill.measurement is not None:
                        # Add to appropriate list based on type
                        if bill.type == "Sign":
                            bill_dict = {
                                "bill_name": bill.name,
                                "bill_unit": bill.type_counting,
                                "quantity": round(bill.measurement, 2),
                                "image": f"{self.URL}media/{bill.image}" if bill.image else "",
                            }
                            sign_bills_list.append(bill_dict)
                        elif bill.type == "Material":
                            bill_dict = {
                                "bill_name": bill.name,
                                "bill_unit": bill.type_counting,
                                "quantity": round(bill.measurement, 2),
                            }
                            detail_bills_list.append(bill_dict)
                
                print(f"DEBUG PdfGenerator - Job {instance.job_id}: sign_bills={len(sign_bills_list)}, detail_bills={len(detail_bills_list)}", flush=True)
                
                if sign_bills_list:
                    new_dict["sign_bills"] = sign_bills_list
                if detail_bills_list:
                    new_dict["detail_bills"] = detail_bills_list
                    
                data.append(new_dict)
            current_site = get_current_site(self.request)

            html_content = {
                "groups": group_name.name if groups else "",
                "context": data,
                "date": date_data,
                "current_site": current_site,
                "site_name": current_site.name,
                "domain": current_site.domain,
                "scheme": request.scheme,
                "single_report": single_report,
            }
            total_time = time.time() - start_time
            logger.info(f"[{request_id}] Detail report complete: {total_time:.2f}s total")
            return Response(html_content, template_name="report.html")

        elif report == "sum_up":
            # Prefetch all bills once instead of querying per group
            all_job_ids = list(instances.values_list("job_id", flat=True))
            all_bills = CloseJobBill.objects.filter(
                job__job_id__in=all_job_ids
            ).select_related("job")
            
            bill_time = time.time() - start_time - query_time
            logger.info(f"[{request_id}] Sum-up bills fetched in {bill_time:.2f}s, {len(all_bills)} bills")
            
            # Calculate aggregates for all bills at once
            
            # Aggregate sign bills by (type, type_counting)
            sign_aggregates = defaultdict(float)
            for bill in all_bills:
                if bill.type == "Sign" and bill.measurement is not None:
                    key = (bill.type, bill.type_counting)
                    value = bill.measurement * (bill.jumping_ration or 1)
                    sign_aggregates[key] += value
            
            # Aggregate material bills by (name, type_counting)
            material_aggregates = defaultdict(float)
            for bill in all_bills:
                if bill.type == "Material" and bill.measurement is not None:
                    key = (bill.name, bill.type_counting)
                    value = bill.measurement * (bill.jumping_ration or 1)
                    material_aggregates[key] += value
            
            group_wise_job_bill_list = []
            job_data_list = []
            
            for group in group_list:
                group_jobs = instances.filter(group_id=group)
                group_job_ids = set(group_jobs.values_list("job_id", flat=True))
                
                # Filter bills for this group in memory
                bill_data_list = [b for b in all_bills if b.job.job_id in group_job_ids]

                for job in group_jobs:
                    if job.further_billing:
                        job_data = {
                            "address": job.job.address,
                            "job_id": job.job.job_id,
                            "notes": job.job.job_notes.all(),
                            "closed_date": job.job.closed_at,
                            "further_billing": job.job.further_billing,
                        }
                        job_data_list.append(job_data)

                bills = []
                list_of_sign_bills = []
                list_of_material_bills = []

                for bill in bill_data_list:
                    bills.append(
                        {
                            "name": bill.name,
                            "type": bill.type,
                            "type_counting": bill.type_counting,
                        },
                    )

                unique_sign = []
                unique_material = []
                unique_keys = set()
                for bill in bills:
                    if bill["type"] == "Sign":
                        key = (bill["type"], bill["type_counting"])
                        if key not in unique_keys:
                            unique_sign.append(bill)
                            unique_keys.add(key)
                    elif bill["type"] == "Material":
                        key = (bill["name"], bill["type_counting"])
                        if key not in unique_keys:
                            unique_material.append(bill)
                            unique_keys.add(key)

                # Use pre-calculated aggregates instead of per-item queries
                for data in unique_sign:
                    key = (data["type"], data["type_counting"])
                    quantity = sign_aggregates.get(key, 0)
                    
                    if data["type_counting"] == TypeCounting.SQM.value:
                        data["name"] = "תמרורים לפי מ״ר (439)"
                    if data["type_counting"] == TypeCounting.UNITS.value:
                        data["name"] = "תמרורים"
                    data["quantity"] = round(quantity, 2)
                    list_of_sign_bills.append(data)

                for data in unique_material:
                    key = (data["name"], data["type_counting"])
                    quantity = material_aggregates.get(key, 0)
                    data["quantity"] = round(quantity, 2)
                    list_of_material_bills.append(data)

                html_content = {
                    "date": date_data,
                    "material": list_of_material_bills,
                    "sign_bill": list_of_sign_bills,
                    "job_data_list": job_data_list,
                    "group_name": group_name,
                }
            total_time = time.time() - start_time
            logger.info(f"[{request_id}] Sum-up report complete: {total_time:.2f}s total")
            return Response(html_content, template_name="sum_up_report.html")

class MultiplePdfGeneratorView(ListAPIView):
    queryset = TransferJob.objects.filter(
        status=JobStatus.CLOSE.value, is_active=True
    ).select_related("job", "group").prefetch_related(
        Prefetch("job__job_image", queryset=JobImage.objects.order_by("id"))
    )
    serializer_class = JobCreationSerializers
    permission_classes = [UserPermission | IsInspector]
    renderer_classes = [TemplateHTMLRenderer]
    URL = ReportGeneratorView.URL
    # Define the parameters
    report = openapi.Parameter(
        "report",
        openapi.IN_QUERY,
        required=False,
        description="report should be detail or sum_up",
        type=openapi.TYPE_STRING,
    )
    from_date = openapi.Parameter(
        "from_date",
        openapi.IN_QUERY,
        required=False,
        type=openapi.TYPE_STRING,
    )
    to_date = openapi.Parameter(
        "to_date",
        openapi.IN_QUERY,
        required=False,
        type=openapi.TYPE_STRING,
    )
    groups = openapi.Parameter(
        "groups",
        openapi.IN_QUERY,
        type=openapi.TYPE_ARRAY,
        items=openapi.Items(type=openapi.TYPE_INTEGER),
        required=False,
    )
    job_ids = openapi.Parameter(
        "job_ids",
        openapi.IN_QUERY,
        type=openapi.TYPE_ARRAY,
        items=openapi.Items(type=openapi.TYPE_INTEGER),
        required=False,
    )
    report_with_image = openapi.Parameter(
        "report_with_image",
        openapi.IN_QUERY,
        type=openapi.TYPE_STRING,
        required=False,
    )
    
    @swagger_auto_schema(
        manual_parameters=[from_date, to_date, groups, job_ids, report_with_image, report]
    )
    def get(self, request, *args, **kwargs):
        start_time = time.time()
        request_id = request.META.get('HTTP_X_REQUEST_ID', 'N/A')
        
        report = self.request.query_params.get("report", None)
        from_date = self.request.query_params.get("from_date", None)
        to_date = self.request.query_params.get("to_date", None)

        from_date_obj = datetime.datetime.strptime(from_date, "%Y-%m-%d")
        to_date_obj = datetime.datetime.strptime(to_date, "%Y-%m-%d")

        date_data = {
            "from_date": from_date_obj.strftime("%d-%m-%Y"),
            "to_date": to_date_obj.strftime("%d-%m-%Y"),
        }
        groups = self.request.query_params.get("groups", None)
        job_ids = self.request.query_params.get("job_ids", None)
        if job_ids is None:
            return Response({"detail":"job ids not found"})
        job_ids_list = [int(x) for x in job_ids.split(",")] if job_ids else []

        report_with_image = self.request.query_params.get("report_with_image", None)
        if groups:
            group_name = Group.objects.get(id=groups)

        if groups:
            group_list = [int(x) for x in groups.split(",")]
            jobs_by_group = self.queryset.filter(group__id__in=group_list)
            job_id_of_main_group = jobs_by_group.filter(
                is_parent_group=True
            ).values_list("job", flat=True)
            child_jobs_of_main_job = TransferJob.objects.filter(
                job_id__in=job_id_of_main_group
            ).values_list("id", flat=True)
            child_jobs_of_group = jobs_by_group.exclude(
                job_id__in=job_id_of_main_group
            ).values_list("id", flat=True)

            transfer_job_id_list = list(child_jobs_of_main_job) + list(
                child_jobs_of_group
            )
        else:
            transfer_job_id_list = list(self.queryset.values_list("id", flat=True))
            group_list = list(self.queryset.distinct().values_list("group", flat=True))
        queryset = (
            TransferJob.objects.filter(id__in=job_ids_list).prefetch_related(
                "job__job_image"
            )
            if job_ids_list
            else self.queryset.filter(id__in=transfer_job_id_list)
        )
        instances = queryset.order_by("-job__closed_at")
        
        query_time = time.time() - start_time
        job_count = instances.count()
        logger.info(f"[{request_id}] MultiplePdfGeneratorView report={report}: Query completed in {query_time:.2f}s, {job_count} jobs")
        
        if report == "detail":
            data = []
            for instance in instances:
                new_dict = {}
                group_data = instance.group
                user_data = instance.group.member.filter(
                    role__title=UserRoleChoices.GROUP_MANAGER.value
                ).values_list("user_name", flat=True)

                new_dict["id"] = instance.job.id
                new_dict["job_id"] = instance.job.job_id
                new_dict["group_name"] = group_data.name
                new_dict["group_manager"] = user_data
                new_dict["address"] = instance.job.address
                new_dict["address_information"] = instance.job.address_information
                if instance.status == JobStatus.CLOSE.value:
                    if instance.job.closed_by:
                        if instance.job.closed_by.user_name:
                            closed_by = instance.job.closed_by.user_name
                            new_dict["close_by"] = closed_by
                        else:
                            closed_by = instance.job.closed_by.email
                            new_dict["close_by"] = closed_by

                    new_dict["notes"] = (
                        instance.job.job_notes.all()
                    )
                    new_dict["description"] = instance.job.description  
                new_dict["closed_at"] = instance.job.closed_at.date() if instance.job.closed_at else instance.job.updated_at.date()

                if report_with_image == "true":
                    extensions = [
                        "mp4",
                        "m4v",
                        "webm",
                        "ogg",
                        "ogv",
                        "MOV",
                        "AVI",
                        "MKV",
                    ]

                    new_dict["images"] = instance.job.job_image.filter(
                        close_job_image=False
                    ).exclude(image__regex="|".join(extensions))
                    new_dict["close_images"] = instance.job.job_image.filter(
                        close_job_image=True
                    ).exclude(image__regex="|".join(extensions))

                # Query bills per instance - EXACTLY like preview does
                instance_bills = CloseJobBill.objects.filter(job__job_id=instance.job_id)
                
                sign_bills_list = []
                detail_bills_list = []

                for bill in instance_bills:
                    print(f"DEBUG MultiplePdf - Job {instance.job_id}: Bill {bill.name}, type={bill.type}, measurement={bill.measurement}", flush=True)
                    if bill.measurement is not None:
                        # Add to appropriate list based on type
                        if bill.type == "Sign":
                            bill_dict = {
                                "bill_name": bill.name,
                                "bill_unit": bill.type_counting,
                                "quantity": round(bill.measurement, 2),
                                "image": f"{self.URL}media/{bill.image}" if bill.image else "",
                            }
                            sign_bills_list.append(bill_dict)
                        elif bill.type == "Material":
                            bill_dict = {
                                "bill_name": bill.name,
                                "bill_unit": bill.type_counting,
                                "quantity": round(bill.measurement, 2),
                            }
                            detail_bills_list.append(bill_dict)
                
                print(f"DEBUG MultiplePdf - Job {instance.job_id}: sign_bills={len(sign_bills_list)}, detail_bills={len(detail_bills_list)}", flush=True)
                
                if sign_bills_list:
                    new_dict["sign_bills"] = sign_bills_list
                if detail_bills_list:
                    new_dict["detail_bills"] = detail_bills_list
                    
                data.append(new_dict)
            current_site = get_current_site(self.request)

            html_content = {
                "groups": group_name.name if groups else "",
                "context": data,
                "date": date_data,
                "current_site": current_site,
                "site_name": current_site.name,
                "domain": current_site.domain,
                "scheme": request.scheme,
                "single_report": True,

            }
            total_time = time.time() - start_time
            logger.info(f"[{request_id}] MultiplePdf detail report complete: {total_time:.2f}s total")
            return Response(html_content, template_name="report.html")

        elif report == "sum_up":
            # Prefetch all bills once instead of querying per group
            all_job_ids = list(instances.values_list("job_id", flat=True))
            all_bills = CloseJobBill.objects.filter(
                job__job_id__in=all_job_ids
            ).select_related("job")
            
            bill_time = time.time() - start_time - query_time
            logger.info(f"[{request_id}] MultiplePdf sum-up bills fetched in {bill_time:.2f}s, {len(all_bills)} bills")
            
            # Calculate aggregates for all bills at once
            
            # Aggregate sign bills by (type, type_counting)
            sign_aggregates = defaultdict(float)
            for bill in all_bills:
                if bill.type == "Sign" and bill.measurement is not None:
                    key = (bill.type, bill.type_counting)
                    value = bill.measurement * (bill.jumping_ration or 1)
                    sign_aggregates[key] += value
            
            # Aggregate material bills by (name, type_counting)
            material_aggregates = defaultdict(float)
            for bill in all_bills:
                if bill.type == "Material" and bill.measurement is not None:
                    key = (bill.name, bill.type_counting)
                    value = bill.measurement * (bill.jumping_ration or 1)
                    material_aggregates[key] += value
            
            group_wise_job_bill_list = []

            job_data_list = []
            for group in group_list:
                group_jobs = instances.filter(group_id=group)
                group_job_ids = set(group_jobs.values_list("job_id", flat=True))
                
                # Filter bills for this group in memory
                bill_data_list = [b for b in all_bills if b.job.job_id in group_job_ids]

                for job in group_jobs:
                    if job.further_billing:
                        job_data = {
                            "address": job.job.address,
                            "job_id": job.job.job_id,
                            "notes": job.job.job_notes.all(),
                            "closed_date": job.job.closed_at,
                            "further_billing": job.job.further_billing,
                        }
                        job_data_list.append(job_data)

                bills = []
                list_of_sign_bills = []
                list_of_material_bills = []

                for bill in bill_data_list:
                    bills.append(
                        {
                            "name": bill.name,
                            "type": bill.type,
                            "type_counting": bill.type_counting,
                        },
                    )

                unique_sign = []
                unique_material = []
                unique_keys = set()
                for bill in bills:
                    if bill["type"] == "Sign":
                        key = (bill["type"], bill["type_counting"])
                        if key not in unique_keys:
                            unique_sign.append(bill)
                            unique_keys.add(key)
                    elif bill["type"] == "Material":
                        key = (bill["name"], bill["type_counting"])
                        if key not in unique_keys:
                            unique_material.append(bill)
                            unique_keys.add(key)

                # Use pre-calculated aggregates instead of per-item queries
                for data in unique_sign:
                    key = (data["type"], data["type_counting"])
                    quantity = sign_aggregates.get(key, 0)
                    
                    if data["type_counting"] == TypeCounting.SQM.value:
                        data["name"] = "Signs per sqm"
                    if data["type_counting"] == TypeCounting.UNITS.value:
                        data["name"] = "Signs"
                    data["quantity"] = round(quantity, 2)
                    list_of_sign_bills.append(data)

                for data in unique_material:
                    key = (data["name"], data["type_counting"])
                    quantity = material_aggregates.get(key, 0)
                    data["quantity"] = round(quantity, 2)
                    list_of_material_bills.append(data)

                html_content = {
                    "date": date_data,
                    "material": list_of_material_bills,
                    "sign_bill": list_of_sign_bills,
                    "job_data_list": job_data_list,
                    "group_name": group_name,
                }
            total_time = time.time() - start_time
            logger.info(f"[{request_id}] MultiplePdf sum-up report complete: {total_time:.2f}s total")
            return Response(html_content, template_name="sum_up_report.html")

class OpenJobPdfGeneratorView(ListAPIView):
    serializer_class = JobCreationSerializers
    permission_classes = [UserPermission | IsInspector]
    renderer_classes = [TemplateHTMLRenderer]
    URL = ReportGeneratorView.URL

    single_job_id = openapi.Parameter(
        "single_job_id",
        openapi.IN_QUERY,
        type=openapi.TYPE_ARRAY,
        items=openapi.Items(type=openapi.TYPE_INTEGER),
        required=True,
    )

    report_with_image = openapi.Parameter(
        "report_with_image",
        openapi.IN_QUERY,
        type=openapi.TYPE_BOOLEAN,
        required=True,
    )

    @swagger_auto_schema(
        manual_parameters=[single_job_id, report_with_image]
    )
    def get(self, request, *args, **kwargs):
        
        job_ids = self.request.query_params.get("job_ids", None)
        job_ids = job_ids.split(",")

        report_with_image = self.request.query_params.get("report_with_image", "false").lower() == "true"
        # Create a Case/When clause to preserve the order of job_ids
        order_case = Case(
            *[When(id=job_id, then=pos) for pos, job_id in enumerate(job_ids)]
        )
        instances = TransferJob.objects.filter(id__in=job_ids).prefetch_related("job__job_image").order_by(order_case) 

        data = []
        for instance in instances:
            new_dict = {}
            user_data = instance.group.member.filter(
                role__title=UserRoleChoices.GROUP_MANAGER.value
            ).values_list("user_name", flat=True)

            new_dict["id"] = instance.job.id
            new_dict["job_id"] = instance.job.job_id
            new_dict["description"] = instance.job.description
            new_dict["group_manager"] = user_data
            new_dict["address"] = instance.job.address
            new_dict["address_information"] = instance.job.address_information
            if instance.status == JobStatus.OPEN.value:
                if instance.job.created_by:
                    if instance.job.created_by.user_name:
                        created_by = instance.job.created_by.user_name
                        new_dict["created_by"] = created_by
                    else:
                        created_by = instance.job.created_by.email
                        new_dict["created_by"] = created_by

            new_dict["updated_at"] = instance.job.updated_at.date()
            new_dict["notes"] = (
                        instance.job.job_notes.all()
                    )

            if report_with_image:
                extensions = [
                    "mp4",
                    "m4v",
                    "webm",
                    "ogg",
                    "ogv",
                    "MOV",
                    "AVI",
                    "MKV",
                ]

                new_dict["images"] = instance.job.job_image.exclude(image__regex="|".join(extensions))
            data.append(new_dict)
        current_site = get_current_site(self.request)

        html_content = {
            "context": data,
            "current_site": current_site,
            "site_name": current_site.name,
            "domain": current_site.domain,
            "scheme": request.scheme,
        }
        return Response(html_content, template_name="open_job_report.html")

        
class CloseJobBillView(viewsets.ModelViewSet):
    queryset = CloseJobBill.objects.all()
    serializer_class = CloseJobBillSerializer
    parser_classes = [MultiPartParser]
    filter_backends = [filters.SearchFilter]
    search_fields = ["name"]

    view_permissions = {
        "list": {"admin": True, "inspector": True, "group_manger": True},
        "create": {"admin": True, "group_manger": True},
        "partial_update": {"admin": True},
        "retrieve": {"admin": True, "inspector": True, "group_manger": True},
    }

    def get_serializer_class(self):
        if self.request.method == "PATCH":
            serializer = CloseJobBillUpdateSerializers
            return serializer
        else:
            return self.serializer_class

    type = openapi.Parameter(
        "bill_type",
        openapi.IN_QUERY,
        required=False,
        description="Bill Type should be Material or Sign",
        type=openapi.TYPE_STRING,
    )

    @swagger_auto_schema(manual_parameters=[type])
    def list(self, request, *args, **kwargs):
        bill_type = self.request.query_params.get("type", None)
        if bill_type:
            queryset = self.paginate_queryset(
                self.filter_queryset(self.queryset.filter(type=bill_type)).order_by(
                    "-created_at"
                )
            )
        else:
            queryset = self.paginate_queryset(
                self.filter_queryset(self.queryset).order_by("-created_at")
            )
        serializer = self.serializer_class(
            queryset, many=True, context={"request": request}
        )
        return self.get_paginated_response(serializer.data)

    def partial_update(self, request, *args, **kwargs):
        kwargs["partial"] = True

        return self.update(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        pk = kwargs.get("pk")
        bill = CloseJobBill.objects.filter(id=pk).first()
        if not bill:
            # {"detail": "Bill not found"}
            return Response(
                {"detail": "סעיף לא נמצא"}, status=return_status.HTTP_404_NOT_FOUND
            )
        serializer = self.serializer_class(instance=bill, context={"request": request})
        return Response(serializer.data, status=return_status.HTTP_200_OK)


class AddDuplicateJobReference(CreateAPIView):
    queryset = TransferJob.objects.all()
    permission_classes = [IsAuthenticated]

    def post(self, request, *args, **kwargs):
        original_job = request.data.get("original_job_id")
        duplicate_job = request.data.get("duplicate_job_id")

        original_job = self.queryset.filter(id=original_job).first()
        transfer_obj = get_object_or_404(TransferJob, id=duplicate_job)

        reference_job = original_job.job.get_duplicate_reference()
        reference_job = (
            ", " + transfer_obj.job.job_id if reference_job else transfer_obj.job.job_id
        )

        original_job.job.set_duplicate_reference(reference_job)
        original_job.job.save()

        # delete duplicate job
        duplicate_obj = get_object_or_404(ReturnJob, duplicate_id=duplicate_job)
        duplicate_obj.delete()

        # delete transfer job
        transfer_obj.delete()
        transfer_obj.job.delete()

        return Response(
            {"detail": "Confirm duplicate successful"}, status=return_status.HTTP_200_OK
        )


class JobIsReviewed(CreateAPIView):
    queryset = TransferJob.objects.all()
    permission_classes = [IsInspector]

    def post(self, request, *args, **kwargs):
        job_id = request.data.get("job_id")
        transferJob = self.queryset.filter(id=job_id).first()
        if request.data.get("is_reviewed") == "true":
            transferJob.is_reviewed = True
        if request.data.get("is_reviewed") == "false":
            transferJob.is_reviewed = False
        transferJob.save()
        return Response(
            {"detail": "Job Reviewed successful"}, status=return_status.HTTP_200_OK
        )


class DeleteJobView(DestroyAPIView):
    queryset = Job.objects.all()
    permission_classes = [IsSuperUser | UserPermission]


class MultipleJobTransferView(CreateAPIView):
    model = TransferJob
    permission_classes = [IsAuthenticated]

    def post(self, request, *args, **kwargs):
        jobs_list = request.data.getlist("jobs")
        transfer_group = request.data.get("group")
        create_list = []

        jobs = TransferJob.objects.filter(job__job_id__in=jobs_list, is_active=True)

        if jobs and len(jobs) == len(jobs_list):
            for job in jobs:
                if TransferJob.objects.filter(group=transfer_group, job=job.job_id):
                    job_transferred_groups = TransferJob.objects.filter(job=job.job_id)
                    bulk_updated_fields = []
                    for job_transferred in job_transferred_groups:
                        job_transferred.status = JobStatus.TRANSFER.value
                        job_transferred.is_active = False
                        bulk_updated_fields.append(job_transferred)

                    TransferJob.objects.bulk_update(
                        bulk_updated_fields, ["status", "is_active"]
                    )

                    TransferJob.objects.filter(
                        group=transfer_group, job=job.job_id
                    ).update(status=JobStatus.OPEN.value, is_active=True)
                else:
                    job.status = JobStatus.TRANSFER.value
                    job.job.status = JobStatus.TRANSFER.value
                    job.is_active = False
                    job.save()
                    job.job.save()

                    create_list.append(
                        TransferJob(
                            created_by_id=self.request.user.id,
                            group_id=transfer_group,
                            job_id=job.job_id,
                            status=JobStatus.TRANSFER.value,
                            is_active=True,
                        )
                    )
                job_log = JobLog.objects.create(
                        job=job.job,
                        transferred_by=request.user,
                        status="Transfer",
                        created_at=timezone.now()
                    )

            TransferJob.objects.bulk_create(create_list)
            return Response(
                {"detail": "Job Transferd successfully"},
                status=return_status.HTTP_200_OK,
            )
        else:
            return Response(
                {"detail": "Job not Transferd successfully"},
                status=return_status.HTTP_404_NOT_FOUND,
            )
