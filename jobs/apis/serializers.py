import requests
from django.conf import settings
from django.db.models import Q
from rest_framework import serializers

from bills.apis.serializers import BillSerializers
from forms.apis.serializers import FormSerializer
from users.apis import serializers as user_serializers
from users.models.group import Group
from users.models.job import CloseJobBill
from users.models.job import Job
from users.models.job import JobAttachment
from users.models.job import JobImage
from users.models.job import JobLog
from users.models.job import JobNote
from users.models.job import JobStatus
from users.models.job import RecentSearchJob
from users.models.job import ReturnJob
from users.models.job import TransferJob
from users.models.notification import Notification


class TransferJobSerializers(serializers.ModelSerializer):
    id = serializers.SerializerMethodField()
    job_id = serializers.SerializerMethodField()
    added_by = serializers.SerializerMethodField()
    closed_by = serializers.SerializerMethodField()
    images = serializers.SerializerMethodField()
    attachments = serializers.SerializerMethodField()
    form = serializers.SerializerMethodField()
    bill = serializers.SerializerMethodField()
    close_job_bill = serializers.SerializerMethodField()
    return_job = serializers.SerializerMethodField()
    transfer_to = serializers.SerializerMethodField()
    group_forms = serializers.SerializerMethodField()
    chat_id = serializers.SerializerMethodField()
    transfer_job_id = serializers.SerializerMethodField()
    address = serializers.SerializerMethodField()
    address_information = serializers.SerializerMethodField()
    group = serializers.SerializerMethodField()
    description = serializers.SerializerMethodField()
    latitude = serializers.SerializerMethodField()
    longitude = serializers.SerializerMethodField()
    priority = serializers.SerializerMethodField()
    main_group = serializers.SerializerMethodField()
    duplicate_reference = serializers.SerializerMethodField()
    updated_by = serializers.SerializerMethodField()
    notes = serializers.SerializerMethodField()
    closed_at = serializers.SerializerMethodField()
    logs = serializers.SerializerMethodField()

    class Meta:
        model = TransferJob
        exclude = ("created_by", "job")

    def get_duplicate_reference(self, obj):
        if obj.job.duplicate_reference:
            return obj.job.duplicate_reference

    def get_priority(self, obj):
        return obj.job.priority

    def get_notes(self, obj):
        job_notes = JobNote.objects.filter(job=obj.job.id)
        return JobNoteSerializer(
            job_notes, many=True, read_only=True, context=self.context
        ).data

    def get_address(self, obj):
        return obj.job.address

    def get_address_information(self, obj):
        return obj.job.address_information

    def get_description(self, obj):
        return obj.job.description

    def get_latitude(self, obj):
        return obj.job.latitude

    def get_longitude(self, obj):
        return obj.job.longitude

    def get_id(self, obj):
        return obj.job.id

    def get_job_id(self, obj):
        return obj.job.job_id

    def get_closed_at(self, obj):
        return obj.job.closed_at

    def get_transfer_job_id(self, obj):
        return obj.id

    def get_group(self, obj):
        return {"id": obj.group.id, "name": obj.group.name}

    def get_images(self, obj):
        job_images = JobImage.objects.filter(job=obj.job.id)
        return JobImagesSerializer(
            job_images, many=True, read_only=True, context=self.context
        ).data

    def get_attachments(self, obj):
        job_attachment = JobAttachment.objects.filter(job=obj.job.id)
        return JobAttachmentSerializer(
            job_attachment, many=True, read_only=True, context=self.context
        ).data

    def get_added_by(self, obj):
        return user_serializers.GetCustomUserSerializers(
            obj.job.created_by, context=self.context
        ).data

    def get_close_job_bill(self, obj):
        transfer_job_id = TransferJob.objects.filter(job_id=obj.job_id).first()
        instance = CloseJobBill.objects.filter(job__job_id=transfer_job_id.job_id)
        return CloseJobBillSerializer(instance, many=True, context=self.context).data

    def get_form(self, obj):
        return FormSerializer(
            obj.job.form, many=True, read_only=True, context=self.context
        ).data

    def get_closed_by(self, obj):
        if obj.status == JobStatus.CLOSE.value:
            if obj.job.closed_by:
                return user_serializers.GetCustomUserSerializers(
                    obj.job.closed_by, context=self.context
                ).data

    def get_updated_by(self, obj):
        if obj.job.updated_by:
            return user_serializers.GetCustomUserSerializers(
                obj.job.updated_by, context=self.context
            ).data

    def get_bill(self, obj):
        return BillSerializers(obj.job.bill, many=True, context=self.context).data

    def get_return_job(self, obj):
        return_job = ReturnJob.objects.filter(  # Done
            Q(job=obj) | Q(duplicate_id=obj.id)
        )
        return ReturnJobDataSerializers(
            return_job, many=True, read_only=True, context=self.context
        ).data

    def get_transfer_to(self, obj):
        if obj:
            transfer_job = (
                TransferJob.objects.filter(job=obj.job)
                .values_list("group", flat=True)
                .order_by("created_at")
            )
            groups = []
            for transfer in transfer_job:
                group = Group.objects.filter(id=transfer)
                groups.extend(group)
            return JobGroupSerializers(groups, many=True, context=self.context).data

    def get_main_group(self, obj):
        return TransferJob.objects.filter(job=obj.job, is_parent_group=True).values(
            "group_id", "group__name"
        )

    def get_group_forms(self, obj):
        return FormSerializer(
            obj.group.form.all(), many=True, context=self.context
        ).data

    def get_chat_id(self, obj):
        return obj.group.chats.first().id if obj.group.chats.first() else None
    
    def get_logs(self, obj):
        logs = obj.job.job_logs.all()
        return JobLogSerializer(logs, many=True, context=self.context
        ).data


class GetTransferJobSerializers(serializers.ModelSerializer):
    id = serializers.SerializerMethodField()
    job_id = serializers.SerializerMethodField()
    added_by = serializers.SerializerMethodField()
    images = serializers.SerializerMethodField()
    attachments = serializers.SerializerMethodField()
    notes = serializers.SerializerMethodField()
    transfer_to = serializers.SerializerMethodField()
    chat_id = serializers.SerializerMethodField()
    transfer_job_id = serializers.SerializerMethodField()
    group = serializers.SerializerMethodField()
    address = serializers.SerializerMethodField()
    address_information = serializers.SerializerMethodField()
    description = serializers.SerializerMethodField()
    latitude = serializers.SerializerMethodField()
    longitude = serializers.SerializerMethodField()
    priority = serializers.SerializerMethodField()
    closed_at = serializers.SerializerMethodField()

    class Meta:
        model = TransferJob
        exclude = ("updated_at", "created_by", "updated_by", "job")

    def get_closed_at(self, obj):
        return obj.job.closed_at

    def get_job_id(self, obj):
        return obj.job.job_id

    def get_priority(self, obj):
        return obj.job.priority

    def get_group(self, obj):
        return {"id": obj.group.id, "name": obj.group.name}

    def get_address(self, obj):
        return obj.job.address

    def get_latitude(self, obj):
        return obj.job.latitude

    def get_longitude(self, obj):
        return obj.job.longitude

    def get_address_information(self, obj):
        return obj.job.address_information

    def get_description(self, obj):
        return obj.job.description

    def get_latitude(self, obj):
        return obj.job.latitude

    def get_longitude(self, obj):
        return obj.job.longitude

    def get_id(self, obj):
        return obj.job.id

    def get_transfer_job_id(self, obj):
        return obj.id

    def get_images(self, obj):
        job_images = JobImage.objects.filter(job=obj.job.id)
        return JobImagesSerializer(
            job_images, many=True, read_only=True, context=self.context
        ).data
    
    def get_notes(self, obj):
        job_notes = JobNote.objects.filter(job=obj.job.id)
        return JobNoteSerializer(
            job_notes, many=True, read_only=True, context=self.context
        ).data

    def get_attachments(self, obj):
        job_attachment = JobAttachment.objects.filter(job=obj.job.id)
        return JobAttachmentSerializer(
            job_attachment, many=True, read_only=True, context=self.context
        ).data

    def get_added_by(self, obj):
        return user_serializers.GetUserSerializers(
            obj.job.created_by, context=self.context
        ).data

    def get_transfer_to(self, obj):
        if obj:
            transfer_job = (
                TransferJob.objects.filter(job=obj.job)
                .values_list("group", flat=True)
                .order_by("created_at")
            )
            groups = []
            for transfer in transfer_job:
                group = Group.objects.filter(id=transfer)
                groups.extend(group)
            return JobGroupSerializers(groups, many=True, context=self.context).data

    def get_chat_id(self, obj):
        return obj.group.chats.first().id if obj.group.chats.first() else None


class JobImagesSerializer(serializers.ModelSerializer):
    class Meta:
        model = JobImage
        fields = ["id", "image", "close_job_image"]

class JobNoteSerializer(serializers.ModelSerializer):
    created_by = serializers.SerializerMethodField()

    class Meta:
        model = JobNote
        fields = ["id", "note", "created_by", "created_at", "updated_at"]

    def get_created_by(self, obj):
        return user_serializers.GetCustomUserSerializers(
            obj.created_by, context=self.context).data

class JobNoteCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = JobNote
        fields = ["id", "note"]


class JobAttachmentSerializer(serializers.ModelSerializer):
    attachment = serializers.SerializerMethodField()

    class Meta:
        model = JobAttachment
        fields = ["id", "attachment", "close_job_attachment"]

    def get_attachment(self, obj):
        request = self.context.get("request")
        attachment_url = str(obj.attachment)
        s3_url = request.build_absolute_uri(settings.MEDIA_URL + attachment_url)

        response = requests.head(s3_url)
        content_length = response.headers.get("content-length")

        modified_attachment = {
            "file_name": attachment_url.split("/")[-1],
            "url": s3_url,
            "size": content_length,
        }
        return modified_attachment


class CustomJobSerializer(serializers.ModelSerializer):
    created_by = serializers.HiddenField(default=serializers.CurrentUserDefault())
    group = serializers.IntegerField(required=True)

    class Meta:
        model = Job
        exclude = ["closed_by", "notes", "status", "comment", "form", "bill"]


class JobGroupSerializers(serializers.ModelSerializer):
    class Meta:
        model = Group
        fields = ["id", "name"]


class ReturnJobDataSerializers(serializers.ModelSerializer):
    duplicate_job = serializers.SerializerMethodField()

    class Meta:
        model = ReturnJob
        fields = [
            "id",
            "job",
            "comment",
            "status",
            "duplicate",
            "duplicate_job",
        ]

    def get_duplicate_job(self, obj):
        if obj.duplicate:
            job = Job.objects.filter(
                id=obj.duplicate.id,
                id__in=TransferJob.objects.filter(group__is_archive=False).values_list(
                    "job_id", flat=True
                ),
            ).first()
            return JobRetrunSerializers(job, context=self.context).data


class EditJobSerializer(serializers.ModelSerializer):
    created_by = serializers.HiddenField(default=serializers.CurrentUserDefault())
    updated_by = serializers.HiddenField(default=serializers.CurrentUserDefault())
    address = serializers.CharField(required=False)
    address_information = serializers.CharField(required=False)
    description = serializers.CharField(required=False)

    class Meta:
        model = Job
        exclude = ["id", "closed_by", "notes", "status", "comment", "form", "bill"]

    def update(self, instance, validated_data):
        images = self.context["images"]
        attachments = self.context["attachments"]
        created_by = validated_data["created_by"]
        updated_by = validated_data["updated_by"]
        for image in images:
            job_image_serializer = JobImagesSerializer(data={"image": image})
            job_image_serializer.is_valid(raise_exception=True)
        for attachment in attachments:
            job_image_serializer = JobAttachmentSerializer(
                data={"attachment": attachment}
            )
            job_image_serializer.is_valid(raise_exception=True)
        instance = super().update(instance, validated_data)
        if images:
            for image in images:
                JobImage.objects.create(
                    job=instance,
                    image=image,
                    created_by=created_by,
                    updated_by=updated_by,
                )

        for attachment in attachments:
            JobAttachment.objects.create(
                job=instance,
                attachment=attachment,
                created_by=created_by,
                updated_by=updated_by,
            )

        instance.updated_by = self.context["request"].user
        instance.closed_by = self.context["request"].user
        instance.save()
        return instance


class JobCreationSerializers(serializers.ModelSerializer):
    added_by = serializers.SerializerMethodField()
    closed_by = serializers.SerializerMethodField()
    created_by = serializers.HiddenField(default=serializers.CurrentUserDefault())
    images = serializers.SerializerMethodField()
    attachments = serializers.SerializerMethodField()
    notes = serializers.SerializerMethodField()
    form = serializers.SerializerMethodField()
    bill = serializers.SerializerMethodField()
    return_job = serializers.SerializerMethodField()
    transfer_to = serializers.SerializerMethodField()
    group_forms = serializers.SerializerMethodField()
    chat_id = serializers.SerializerMethodField()

    class Meta:
        model = Job
        fields = "__all__"
        extra_kwargs = {"id": {"error_messages": {"invalid": "נדרש מספר שלם חוקי."}}}

    def create(self, validated_data):
        created_by = validated_data["created_by"]
        images = self.context["images"]
        attachments = self.context["attachments"]
        notes = self.context["notes"]

        for image in images:
            job_image_serializer = JobImagesSerializer(data={"image": image})
            job_image_serializer.is_valid(raise_exception=True)
        for attachment in attachments:
            job_image_serializer = JobAttachmentSerializer(
                data={"attachment": attachment}
            )
            job_image_serializer.is_valid(raise_exception=True)
        for note in notes:
            job_note_serializer = JobNoteCreateSerializer(
                data={"note": note}
            )
            job_note_serializer.is_valid(raise_exception=True)

        job = Job.objects.create(**validated_data)
        for form in self.context["forms"]:
            job.form.add(form)
            job.save()
        for bill in self.context["bills"]:
            job.bill.add(bill)
            job.save()
        for image in images:
            JobImage.objects.create(job=job, image=image, created_by=created_by)
        for note in notes:
            JobNote.objects.create(job=job, note=note, created_by=created_by)
        for attachment in attachments:
            JobAttachment.objects.create(
                job=job,
                attachment=attachment,
                created_by=created_by,
            )
        return job

    def get_images(self, obj):
        job_images = JobImage.objects.filter(job=obj)
        return JobImagesSerializer(
            job_images, many=True, read_only=True, context=self.context
        ).data

    def get_attachments(self, obj):
        job_attachment = JobAttachment.objects.filter(job=obj)
        return JobAttachmentSerializer(
            job_attachment, many=True, read_only=True, context=self.context
        ).data
    
    def get_notes(self, obj):
        job_note = JobNote.objects.filter(job=obj)
        return JobNoteSerializer(
            job_note, many=True, read_only=True, context=self.context
        ).data

    def get_added_by(self, obj):
        return user_serializers.GetUserSerializers(
            obj.created_by, context=self.context
        ).data

    def get_form(self, obj):
        return FormSerializer(
            obj.form, many=True, read_only=True, context=self.context
        ).data

    def get_closed_by(self, obj):
        if obj.closed_by:
            return user_serializers.GetUserSerializers(
                obj.closed_by, context=self.context
            ).data

    def get_bill(self, obj):
        return BillSerializers(obj.bill, many=True, context=self.context).data

    def get_return_job(self, obj):
        return_job_id_list = obj.assign_job.filter(
            status=JobStatus.RETURN.value
        ).values_list("id", flat=True)
        return_job = ReturnJob.objects.filter(  # Done
            Q(job_id__in=return_job_id_list) | Q(duplicate_id__in=return_job_id_list)
        ).distinct()
        return ReturnJobDataSerializers(
            return_job, many=True, read_only=True, context=self.context
        ).data

    def get_transfer_to(self, obj):
        transfer_job = (
            TransferJob.objects.filter(job=obj, group__is_archive=False)
            .values_list("group", flat=True)
            .order_by("created_at")
        )
        groups = []
        for transfer in transfer_job:
            group = Group.objects.filter(id=transfer, is_archive=False)
            groups.extend(group)
        return JobGroupSerializers(groups, many=True, context=self.context).data

    def get_group_forms(self, obj):
        return None

    def get_chat_id(self, obj):
        job_group = obj.assign_job.first()
        if job_group:
            return (
                job_group.group.chats.first().id
                if job_group.group.chats.first()
                else None
            )
        return None


class GetJobCreateSerializer(serializers.ModelSerializer):
    transfer_job_id = serializers.SerializerMethodField()
    added_by = serializers.SerializerMethodField()
    created_by = serializers.HiddenField(default=serializers.CurrentUserDefault())
    updated_by = serializers.HiddenField(default=serializers.CurrentUserDefault())
    images = serializers.SerializerMethodField()
    attachments = serializers.SerializerMethodField()
    notes = serializers.SerializerMethodField()
    chat_id = serializers.SerializerMethodField()

    class Meta:
        model = Job
        fields = "__all__"
        extra_kwargs = {"id": {"error_messages": {"invalid": "נדרש מספר שלם חוקי."}}}

    def get_transfer_job_id(self, obj):
        return self.context.get("id")

    def get_images(self, obj):
        job_images = JobImage.objects.filter(job=obj)
        return JobImagesSerializer(
            job_images, many=True, read_only=True, context=self.context
        ).data
    
    def get_notes(self, obj):
        job_notes = JobNote.objects.filter(job=obj)
        return JobNoteSerializer(
            job_notes, many=True, read_only=True, context=self.context
        ).data

    def get_attachments(self, obj):
        job_attachment = JobAttachment.objects.filter(job=obj)
        return JobAttachmentSerializer(
            job_attachment, many=True, read_only=True, context=self.context
        ).data

    def get_added_by(self, obj):
        return user_serializers.GetUserSerializers(
            obj.created_by, context=self.context
        ).data

    def get_chat_id(self, obj):
        job_group = obj.assign_job.first()
        if job_group:
            return (
                job_group.group.chats.first().id
                if job_group.group.chats.first()
                else None
            )
        return None


class UpdateCustomJobSerializer(serializers.ModelSerializer):
    created_by = serializers.HiddenField(default=serializers.CurrentUserDefault())
    updated_by = serializers.HiddenField(default=serializers.CurrentUserDefault())
    closed_by = serializers.HiddenField(default=serializers.CurrentUserDefault())

    class Meta:
        model = Job
        fields = "__all__"


class JobTransferSerializer(serializers.ModelSerializer):
    created_by = serializers.HiddenField(default=serializers.CurrentUserDefault())
    updated_by = serializers.HiddenField(default=serializers.CurrentUserDefault())

    class Meta:
        model = TransferJob
        fields = (
            "group",
            "job",
            "created_by",
            "updated_by",
            "status",
            "further_inspection",
            "further_billing",
            "is_lock_closed"
        )

    def to_representation(self, instance):
        job_list = instance.job
        return GetJobCreateSerializer(
            job_list, context={"id": instance.id, "request": self.context["request"]}
        ).data


class JobRetrunSerializers(serializers.ModelSerializer):
    images = serializers.SerializerMethodField()
    attachments = serializers.SerializerMethodField()
    added_by = serializers.SerializerMethodField()
    group = serializers.SerializerMethodField()
    chat_id = serializers.SerializerMethodField()

    class Meta:
        model = Job
        fields = [
            "id",
            "address",
            "created_at",
            "added_by",
            "address_information",
            "description",
            "group",
            "status",
            "chat_id",
            "images",
            "attachments",
            "latitude",
            "longitude",
            "priority",
            "further_inspection",
            "is_lock_closed"
        ]

    def get_added_by(self, obj):
        return user_serializers.GetUserSerializers(
            obj.created_by, context=self.context
        ).data

    def get_group(self, obj):
        job_group = TransferJob.objects.filter(job=obj.id, is_parent_group=True).first()
        return {"id": job_group.group.id, "name": job_group.group.name}

    def get_images(self, obj):
        job_images = JobImage.objects.filter(job=obj)
        return JobImagesSerializer(
            job_images, many=True, read_only=True, context=self.context
        ).data

    def get_attachments(self, obj):
        job_attachment = JobAttachment.objects.filter(job=obj)
        return JobAttachmentSerializer(
            job_attachment, many=True, read_only=True, context=self.context
        ).data

    def get_chat_id(self, obj):
        instance = TransferJob.objects.filter(job=obj.id, is_parent_group=True).first()
        return (
            instance.group.chats.first().id if instance.group.chats.first().id else None
        )


class CustomReturnJobSerializer(serializers.ModelSerializer):
    created_by = serializers.HiddenField(default=serializers.CurrentUserDefault())
    updated_by = serializers.HiddenField(default=serializers.CurrentUserDefault())

    class Meta:
        model = ReturnJob
        exclude = ["return_to"]


class JobSerializers(serializers.ModelSerializer):
    images = serializers.SerializerMethodField()
    attachments = serializers.SerializerMethodField()
    added_by = serializers.SerializerMethodField()
    group = serializers.SerializerMethodField()
    chat_id = serializers.SerializerMethodField()

    class Meta:
        model = Job
        fields = "__all__"
        extra_kwargs = {"id": {"error_messages": {"invalid": "נדרש מספר שלם חוקי."}}}

    def get_added_by(self, obj):
        return user_serializers.GetUserSerializers(
            obj.created_by, context=self.context
        ).data

    def get_group(self, obj):
        job_group = TransferJob.objects.filter(job=obj.id, is_parent_group=True).first()
        return {"id": job_group.group.id, "name": job_group.group.name}

    def get_images(self, obj):
        job_images = JobImage.objects.filter(job=obj).all()
        return JobImagesSerializer(
            job_images, many=True, read_only=True, context=self.context
        ).data

    def get_attachments(self, obj):
        job_attachment = JobAttachment.objects.filter(job=obj).all()
        return JobAttachmentSerializer(
            job_attachment, many=True, read_only=True, context=self.context
        ).data

    def get_chat_id(self, obj):
        instance = TransferJob.objects.filter(job=obj.id, is_parent_group=True).first()
        return (
            instance.group.chats.first().id if instance.group.chats.first().id else None
        )


class ReturnJobListSerializer(serializers.ModelSerializer):
    created_by = serializers.HiddenField(default=serializers.CurrentUserDefault())
    updated_by = serializers.HiddenField(default=serializers.CurrentUserDefault())
    duplicates = serializers.SerializerMethodField()
    transfer_to = serializers.SerializerMethodField()
    job = serializers.SerializerMethodField()
    transfer_job_id_duplicate = serializers.SerializerMethodField()
    transfer_job_id = serializers.SerializerMethodField()

    class Meta:
        model = ReturnJob
        fields = "__all__"

    def get_transfer_to(self, obj):
        transfer_to = (
            TransferJob.objects.filter(job=obj.job.job, group__is_archive=False)
            .values_list("group", flat=True)
            .order_by("created_at")
        )
        groups = []
        for transfer in transfer_to:
            group = Group.objects.filter(id=transfer, is_archive=False)
            groups.extend(group)
        return JobGroupSerializers(groups, many=True, context=self.context).data

    def get_duplicates(self, instance):
        if instance.duplicate:
            return JobRetrunSerializers(
                instance.duplicate.job, context=self.context
            ).data

    def get_transfer_job_id_duplicate(self, instance):
        if instance.duplicate:
            return instance.duplicate.id

    def get_transfer_job_id(self, instance):
        return instance.job.id

    def get_job(self, instance):
        job_list = instance.job.job
        return JobSerializers(job_list, context=self.context).data


class ReturnJobSerializer(serializers.ModelSerializer):
    created_by = serializers.HiddenField(default=serializers.CurrentUserDefault())
    updated_by = serializers.HiddenField(default=serializers.CurrentUserDefault())
    return_job = serializers.SerializerMethodField()
    # jobs = serializers.SerializerMethodField()
    duplicates = serializers.SerializerMethodField()

    class Meta:
        model = ReturnJob
        fields = "__all__"

    # def get_jobs(self, obj):
    #     return JobRetrunSerializers(obj.job, context=self.context).data

    def get_return_job(self, instance):
        retun_job = ReturnJob.objects.filter(job=instance.job)
        return ReturnJobDataSerializers(
            retun_job.job, many=True, read_only=True, context=self.context
        ).data

    def get_duplicates(self, instance):
        if instance.duplicate:
            return JobRetrunSerializers(
                instance.duplicate.job, context=self.context
            ).data

    def to_representation(self, instance):
        job_list = instance.job
        return GetTransferJobSerializers(job_list, context=self.context).data


class JobNotificationSerializers(serializers.ModelSerializer):
    images = serializers.SerializerMethodField()
    transfer_job_id = serializers.SerializerMethodField()

    class Meta:
        model = Job
        fields = ["images", "address", "transfer_job_id"]

    def get_images(self, obj):
        job_images = JobImage.objects.filter(job=obj)
        return JobImagesSerializer(
            job_images, many=True, read_only=True, context=self.context
        ).data

    def get_transfer_job_id(self, obj):
        transfer_job_id = self.context.get("transfer_job_id")
        if transfer_job_id:
            return transfer_job_id
        return None


class NotificationSerializer(serializers.ModelSerializer):
    jobs = serializers.SerializerMethodField()
    senders = serializers.SerializerMethodField()

    class Meta:
        model = Notification
        fields = "__all__"

    def get_jobs(self, obj):
        self.context["transfer_job_id"] = obj.job.id
        return JobNotificationSerializers(obj.job.job, context=self.context).data

    def get_senders(self, obj):
        return user_serializers.GetUserSerializers(
            obj.sender, context=self.context
        ).data


class RecentSearchJobSerializer(serializers.ModelSerializer):
    images = serializers.SerializerMethodField()

    class Meta:
        model = Job
        fields = [
            "id",
            "address",
            "address_information",
            "description",
            "latitude",
            "longitude",
            "images",
            "status",
            "created_at",
        ]

    def get_images(self, obj):
        job_images = JobImage.objects.filter(job=obj)
        return JobImagesSerializer(
            job_images, many=True, read_only=True, context=self.context
        ).data


class RecentSearchJobCreateSerializer(serializers.ModelSerializer):
    jobs = serializers.SerializerMethodField()

    class Meta:
        model = RecentSearchJob
        fields = "__all__"

    def get_jobs(self, obj):
        return RecentSearchJobSerializer(obj.job.job, context=self.context).data


class GetCustomGroupSerializer(serializers.ModelSerializer):
    class Meta:
        model = Group
        fields = [
            "id",
            "name",
            "created_at",
        ]


class CloseJobBillSerializer(serializers.ModelSerializer):
    created_by = serializers.HiddenField(default=serializers.CurrentUserDefault())
    updated_by = serializers.HiddenField(default=serializers.CurrentUserDefault())

    class Meta:
        model = CloseJobBill
        fields = "__all__"

    # def create(self, validated_data):
    #     instance = super().create(validated_data)
    #     if "image" in validated_data:
    #         image = validated_data.pop("image")
    #         instance.image = image
    #         instance.save()
    #     return instance


class CloseJobBillUpdateSerializers(serializers.ModelSerializer):
    updated_by = serializers.HiddenField(default=serializers.CurrentUserDefault())
    type_counting = serializers.CharField(required=False)
    name = serializers.CharField(required=False)

    class Meta:
        model = CloseJobBill
        exclude = ["job", "created_by"]

    def update(self, instance, validated_data):
        instance = super().update(instance, validated_data)
        instance.updated_by = self.context["request"].user
        instance.save()
        return instance


class GroupByJobSerializer(serializers.ModelSerializer):
    id = serializers.SerializerMethodField()
    job_id = serializers.SerializerMethodField()
    images = serializers.SerializerMethodField()
    transfer_to = serializers.SerializerMethodField()
    chat_id = serializers.SerializerMethodField()
    transfer_job_id = serializers.SerializerMethodField()
    group = serializers.SerializerMethodField()
    address = serializers.SerializerMethodField()
    address_information = serializers.SerializerMethodField()
    description = serializers.SerializerMethodField()
    latitude = serializers.SerializerMethodField()
    longitude = serializers.SerializerMethodField()
    priority = serializers.SerializerMethodField()
    closed_at = serializers.SerializerMethodField()

    class Meta:
        model = TransferJob
        exclude = (
            "updated_at",
            "created_by",
            "updated_by",
            "job",
        )

    def get_closed_at(self, obj):
        return obj.job.closed_at

    def get_job_id(self, obj):
        return obj.job.job_id

    def get_priority(self, obj):
        return obj.job.priority

    def get_group(self, obj):
        return {"id": obj.group.id, "name": obj.group.name}

    def get_address(self, obj):
        return obj.job.address

    def get_latitude(self, obj):
        return obj.job.latitude

    def get_longitude(self, obj):
        return obj.job.longitude

    def get_address_information(self, obj):
        return obj.job.address_information

    def get_description(self, obj):
        return obj.job.description

    def get_latitude(self, obj):
        return obj.job.latitude

    def get_longitude(self, obj):
        return obj.job.longitude

    def get_id(self, obj):
        return obj.job.id

    def get_transfer_job_id(self, obj):
        return obj.id

    def get_images(self, obj):
        job_images = JobImage.objects.filter(job=obj.job.id)
        return JobImagesSerializer(
            job_images, many=True, read_only=True, context=self.context
        ).data

    def get_transfer_to(self, obj):
        if obj:
            transfer_job = (
                TransferJob.objects.filter(job=obj.job)
                .values_list("group", flat=True)
                .order_by("created_at")
            )
            groups = []
            for transfer in transfer_job:
                group = Group.objects.filter(id=transfer)
                groups.extend(group)
            return JobGroupSerializers(groups, many=True, context=self.context).data

    def get_chat_id(self, obj):
        return obj.group.chats.first().id if obj.group.chats.first() else None


class JobLogSerializer(serializers.ModelSerializer):
    created_by = serializers.SerializerMethodField()
    updated_by = serializers.SerializerMethodField()
    transferred_by = serializers.SerializerMethodField()
    returned_by = serializers.SerializerMethodField()
    closed_by = serializers.SerializerMethodField()
    partially_closed_by = serializers.SerializerMethodField()

    class Meta:
        model = JobLog
        fields = ['id', 'job', 'status', 'created_by', 'updated_by', 'transferred_by', 'returned_by', 'closed_by', 'created_at', 'partially_closed_by']

    def get_created_by(self, obj):
        return user_serializers.GetCustomUserSerializers(
            obj.created_by, context=self.context).data
    
    def get_updated_by(self, obj):
        return user_serializers.GetCustomUserSerializers(
            obj.updated_by, context=self.context).data
    
    def get_transferred_by(self, obj):
        return user_serializers.GetCustomUserSerializers(
            obj.transferred_by, context=self.context).data
    
    def get_returned_by(self, obj):
        return user_serializers.GetCustomUserSerializers(
            obj.returned_by, context=self.context).data
    
    def get_closed_by(self, obj):
        return user_serializers.GetCustomUserSerializers(
            obj.closed_by, context=self.context).data
    
    def get_partially_closed_by(self, obj):
        return user_serializers.GetCustomUserSerializers(
            obj.partially_closed_by, context=self.context).data
    