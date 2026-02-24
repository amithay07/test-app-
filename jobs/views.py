import datetime
import decimal
import json
import logging
import os
import time
from datetime import datetime
from datetime import timedelta
from itertools import chain

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.core.paginator import EmptyPage
from django.core.paginator import PageNotAnInteger
from django.core.paginator import Paginator
from django.db import IntegrityError
from django.db import transaction
from django.db.models import Case
from django.db.models import CharField
from django.db.models import Count
from django.db.models import F
from django.db.models import FloatField
from django.db.models import OuterRef
from django.db.models import Q
from django.db.models import Subquery
from django.db.models import Sum
from django.db.models import Value
from django.db.models import When
from django.db.models.functions import Coalesce
from django.forms.models import model_to_dict
from django.forms.models import modelform_factory
from django.http import FileResponse
from django.http import HttpResponse
from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from django.template.loader import get_template
from django.urls import reverse_lazy
from django.utils import timezone
from django.utils.decorators import method_decorator
from django.utils.translation import gettext_lazy as _
from django.views.generic import CreateView
from django.views.generic import DeleteView
from django.views.generic import ListView
from django.views.generic import TemplateView
from django.views.generic import UpdateView
from django.views.generic import View
from weasyprint import HTML

from bills.forms import CloseBillForm
from jobs.forms import CreateJobForm
from jobs.forms import ReturnJobForm
from jobs.forms import ReturnJobNotesForm
from jobs.forms import TransferJobForm
from users.models import UserRoleChoices
from users.models.bill import Bill
from users.models.bill import BillType
from users.models.bill import TypeCounting
from users.models.form import Form
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
from users.models.notification import ChatNotification
from users.models.notification import Notification
from users.models.user import UserRole
from users.utils import get_query_params


logger = logging.getLogger(__name__)


imageVideoExtensions = [
    "png",
    "jpg",
    "jpeg",
    "mp4",
    "mkv",
    "m4a",
    "mov",
    "wmv",
    "avi",
    "avchd",
    "flv",
    "f4v",
    "swf",
    "webm",
    "mpeg-2",
    "html5",
]


def duplicate_job_list(self):
    duplicate_job_list = list(
        TransferJob.objects.filter(is_active=True)
        .prefetch_related("job__job_image")
        .order_by("-created_at")
    )
    return duplicate_job_list


def material_bills_list(self):
    material_bills_list = list(
        Bill.objects.filter(type=BillType.MATERIAL.value).order_by("-created_at")
    )
    return material_bills_list


def sign_bills_list(self):
    sign_bills_list = list(
        Bill.objects.filter(type=BillType.SIGN.value).order_by("-created_at")
    )
    return sign_bills_list


def is_sign(self, job_id):
    is_sign = TransferJob.objects.filter(id=job_id).values_list(
        "group__form__is_sign", flat=True
    )
    return is_sign


# List for Notification Module
def NotificationList(self):
    user_id = self.request.user.id
    seven_days_ago = datetime.now() - timedelta(days=7)

    chat_notification = (
        ChatNotification.objects.filter(
            receiver_id=user_id, created_at__gte=seven_days_ago
        )
        .select_related("sender", "group")
        .order_by("-created_at")
    )
    notification = (
        Notification.objects.filter(receiver_id=user_id, created_at__gte=seven_days_ago)
        .select_related("job__job", "sender")
        .order_by("-created_at")
    )
    notifications = list(chain(chat_notification, notification))
    return notifications


def first_image_subquery(job_field):
    allowed_extensions = ["jpg", "jpeg", "png"]
    return (
        JobImage.objects.filter(job=OuterRef(job_field))
        .annotate(
            file_extension=Case(
                *[
                    When(
                        image__iendswith=ext, then=Value(ext, output_field=CharField())
                    )
                    for ext in allowed_extensions
                ],
                default=Value(None, output_field=CharField()),
            )
        )
        .filter(file_extension__isnull=False)
        .order_by("id")
        .values("image")[:1]
    )


# List for Job Module
@method_decorator(login_required, name="dispatch")
class JobListView(ListView):
    model = TransferJob
    allowed_extensions = ["jpg", "jpeg", "png"]
    template_name = "job.html"
    success_url = reverse_lazy("jobs:job-list")

    def get_queryset(self):

        current_user = self.request.user
        queryset = TransferJob.objects.exclude(group__is_archive=True).select_related(
            "job", "group"
        )

        search = self.request.GET.get("search")

        if search:
            query_filters = (
                Q(job__address__icontains=search)
                | Q(job__address_information__icontains=search)
                | Q(job__job_id__icontains=search)
                | Q(job__duplicate_reference__icontains=search)
            )
            if not current_user.is_superuser:
                query_filters &= Q(group__member=current_user.id)

            queryset = queryset.filter(query_filters)

        return queryset

    def get_context_data(self, **kwargs):
        start = time.time()
        queryset = self.object_list
        current_user = self.request.user

        # Optimize: Create search suggestions from fresh queryset (not paginated)
        # This reduces the IN clause size significantly
        # Limit to prevent loading all jobs into memory
        search_job_suggestions = (
            TransferJob.objects.exclude(group__is_archive=True)
            .filter(is_active=True)
            .select_related("job", "group")
            .prefetch_related("job__job_image")[
                :100
            ]  # Limit to first 100 suggestions for performance
        )

        group = self.request.GET.get("group")
        if group:
            if not current_user.is_superuser:
                group_id = (
                    Group.objects.filter(member=current_user.id, name=group)
                    .exclude(is_archive=True)
                    .values_list("id", flat=True)
                    .first()
                )

            group_id = (
                Group.objects.filter(name=group)
                .exclude(is_archive=True)
                .values_list("id", flat=True)
                .first()
            )
            self.request.session["previous_group_id"] = group_id

        # Retrieve the previous group_id from session
        previous_group_id = self.request.session.get("previous_group_id")
        if previous_group_id:
            queryset = queryset.filter(group__id=previous_group_id)

        context = super().get_context_data(**kwargs)
        context_time = time.time()
        print("Context: {}".format(context_time - start))
        job_type = self.request.GET.get("job", "Open")
        (from_date, to_date) = get_query_params(self.request.GET)
        group = self.request.GET.get("group")
        page_number = self.request.GET.get("page")

        queryset_time = time.time()
        print("queryset: {}".format(queryset_time - context_time))
        group_query = Group.objects.exclude(is_archive=True)

        filter_query = Q()
        if not current_user.is_superuser:
            filter_query &= Q(member=current_user.id)

        group_list = group_query.filter(filter_query)

        group_time = time.time()
        print("group: {}".format(group_time - queryset_time))

        context["group_list"] = group_list

        if not group:
            group_id = self.request.session.get("previous_group_id")
            group = (
                Group.objects.filter(id=group_id).values_list("name", flat=True).first()
            )

        filter_group = group if group else context["group_list"].first()
        context["selected_group"] = filter_group
        context["job_type"] = job_type
        filter_query = queryset
        if filter_group:
            queryset = queryset.filter(group__name=filter_group)
            filter_query = queryset

        sort_by = self.request.GET.get("sort_by")
        if job_type == "Open":
            order = "-created_at"
            if sort_by:
                if sort_by == "ascending":
                    order = "created_at"

            if from_date and to_date:
                queryset = queryset.filter(created_at__date__range=[from_date, to_date])
                filter_query = queryset
            # All Jobs excluding Close-Job and Job with Group-Filter excluding Close-Job
            queryset = queryset.exclude(
                status__in=[JobStatus.CLOSE.value, JobStatus.RETURN.value]
            ).order_by(order)
            close_job = False
        else:
            order = "-job__closed_at"
            if sort_by:
                if sort_by == "ascending":
                    order = "job__closed_at"
            # Close jobs and Close-Job with Group-Filter
            if from_date and to_date:
                queryset = queryset.filter(
                    job__closed_at__date__range=[from_date, to_date]
                )
                filter_query = queryset
            queryset = queryset.filter(status=JobStatus.CLOSE.value).order_by(order)
            close_job = True

        job_type_time = time.time()
        print("job_type: {}".format(job_type_time - group_time))

        # Prefetch job images for the paginated results
        queryset = queryset.prefetch_related("job__job_image")

        # Pagination
        paginator = Paginator(queryset, 20)
        context["paginator"] = paginator
        context["jobs"] = paginator.get_page(page_number)

        try:
            context["page_range"] = paginator.page(page_number)
        except PageNotAnInteger:
            context["page_range"] = paginator.page(1)
        except EmptyPage:
            context["page_range"] = paginator.page(paginator.num_pages)

        # Job Count by status
        job_counts = filter_query.filter(is_active=True).aggregate(
            Open=Count("status", Q(status=JobStatus.OPEN.value)),
            Partial=Count("status", Q(status=JobStatus.PARTIAL.value)),
            Return=Count(
                "status", Q(status=JobStatus.RETURN.value, is_parent_group=True)
            ),
            Transfer=Count("status", Q(status=JobStatus.TRANSFER.value)),
        )

        job_counts_time = time.time()
        print("job_counts: {}".format(job_counts_time - job_type_time))

        context["jobs_count"] = {
            _("Open"): job_counts["Open"],
            _("Partial"): job_counts["Partial"],
            _("Return"): job_counts["Return"],
            _("Transfer"): job_counts["Transfer"],
        }
        context["from_date"] = from_date
        context["to_date"] = to_date
        # context["duplicate_job_list"] = list(queryset.order_by("-created_at"))
        duplicate_job_list_time = time.time()
        print(
            "duplicate_job_list: {}".format(duplicate_job_list_time - job_counts_time)
        )
        context["material_bills_list"] = material_bills_list(self)
        material_bills_list_time = time.time()
        print(
            "material_bills_list: {}".format(
                material_bills_list_time - duplicate_job_list_time
            )
        )
        context["sign_bills_list"] = sign_bills_list(self)
        sign_bills_list_time = time.time()
        print(
            "sign_bills_list: {}".format(
                sign_bills_list_time - material_bills_list_time
            )
        )
        context["google_api_key"] = settings.GOOGLE_API_KEY
        context["close_job"] = close_job
        context["notification"] = NotificationList(self)
        NotificationList_time = time.time()
        print(
            "NotificationList: {}".format(NotificationList_time - sign_bills_list_time)
        )
        context["search_job_suggestions"] = search_job_suggestions

        return_jobs = ReturnJob.objects
        if not current_user.is_superuser:
            return_jobs = return_jobs.filter(
                job__is_active=True, return_to=current_user.id
            )
        context["return_jobs"] = (
            return_jobs.prefetch_related(
                "job__job__job_image", "duplicate__job__job_image"
            )
            .annotate(first_job_image=Subquery(first_image_subquery("job__job")))
            .annotate(
                first_duplicate_job_image=Subquery(
                    first_image_subquery("duplicate__job")
                )
            )
            .order_by("-created_at")
        )
        return_jobs_time = time.time()
        print("return_jobs: {}".format(return_jobs_time - NotificationList_time))

        # Retrieve the previous group_id from session
        previous_group_id = self.request.session.get("previous_group_id")
        if previous_group_id:
            context["previous_group_id"] = previous_group_id
        return context


# List for Dashboard Module
@method_decorator(login_required, name="dispatch")
class DashboardView(JobListView):
    model = Job
    template_name = "dashboard.html"
    success_url = reverse_lazy("index")

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["google_api_key"] = settings.GOOGLE_API_KEY
        return context


# List for Added Job Module
@method_decorator(login_required, name="dispatch")
class AddNewJobListView(ListView):
    model = Job
    template_name = "add_new_job_list.html"
    success_url = reverse_lazy("jobs:add-new-job")
    queryset = TransferJob.objects.filter(is_active=True).exclude(
        group__is_archive=True
    )
    ordering = ["-created_at"]

    def get_queryset(self):
        (from_date, to_date) = get_query_params(self.request.GET)
        search = self.request.GET.get("search")
        user = self.request.user
        queryset = (
            super()
            .get_queryset()
            .filter(
                Q(job__address__icontains=search)
                | Q(job__address_information__icontains=search)
                | Q(job__job_id__icontains=search)
                | Q(job__duplicate_reference__icontains=search),
                created_by=user,
            )
            if search
            else super().get_queryset().filter(created_by=user)
        )
        queryset = (
            queryset.filter(
                created_at__date__range=[from_date, to_date], created_by=user
            )
            if from_date and to_date
            else queryset.filter(created_by=user)
        )
        return queryset

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        (from_date, to_date) = get_query_params(self.request.GET)
        page_number = self.request.GET.get("page")
        current_user = self.request.user
        context["group_list"] = (
            Group.objects.filter(member=current_user.id).exclude(is_archive=True)
            if not current_user.is_superuser
            else (
                Group.objects.exclude(is_archive=True)
                if current_user.is_superuser
                else None
            )
        )
        context["from_date"] = from_date
        context["to_date"] = to_date
        # context["duplicate_job_list"] = duplicate_job_list(self)
        context["material_bills_list"] = material_bills_list(self)
        context["sign_bills_list"] = sign_bills_list(self)
        context["google_api_key"] = settings.GOOGLE_API_KEY

        paginator = Paginator(self.get_queryset(), 10)
        context["paginator"] = paginator
        context["new_jobs"] = paginator.get_page(page_number)
        context["notification"] = NotificationList(self)

        try:
            context["page_range"] = paginator.page(page_number)
        except PageNotAnInteger:
            context["page_range"] = paginator.page(1)
        except EmptyPage:
            context["page_range"] = paginator.page(paginator.num_pages)
        return context


# create job view
class JobCreateView(CreateView):
    model = Job
    template_name = "add_new_job_list.html"
    success_url = reverse_lazy("jobs:add-new-job")
    form_class = modelform_factory(Job, form=CreateJobForm, fields="__all__")

    @transaction.atomic
    def post(self, request, *args, **kwargs):
        user = request.user
        post_data = request.POST.copy()
        post_data["status"] = JobStatus.OPEN.value
        form = self.form_class(post_data, request.FILES)

        images = []
        attachmentes = []
        notes = []
        for file in request.FILES.getlist("attachment"):
            if file.name.split(".")[-1] in imageVideoExtensions:
                images.append(file)
            else:
                attachmentes.append(file)

        for note in request.POST.getlist("note"):
            notes.append(note)

        group_id = request.POST.get("group")
        further_inspection = request.POST.get("further_inspection") == "on"
        is_lock_closed = request.POST.get("is_lock_closed") == "on"
        inspector = (
            Group.objects.filter(id=group_id)
            .exclude(is_archive=True)
            .values_list("member__role_id__title", flat=True)
        )

        # if UserRoleChoices.GROUP_MANAGER.value not in inspector:
        #     return JsonResponse(
        #         {"error": {"group": "אין מנהל קבוצה בקבוצה זו, אנא הוסף מנהל קבוצה."}}
        #     )

        if not form.is_valid():
            return JsonResponse({"error": form.errors if form.errors else None})

        job = form.save()
        if images:
            for image in images:
                JobImage.objects.create(
                    job=job,
                    image=image,
                    created_by=user,
                    updated_by=user,
                )

        if attachmentes:
            for attachment in attachmentes:
                JobAttachment.objects.create(
                    job=job,
                    attachment=attachment,
                    created_by=user,
                    updated_by=user,
                )
        if notes:
            for note in notes:
                JobNote.objects.create(
                    job=job,
                    note=note,
                    created_by=user,
                    updated_by=user,
                )

        job.created_by = user
        job.status = JobStatus.OPEN.value
        job.save()

        transfer_job_obj = TransferJob.objects.create(
            status=JobStatus.OPEN.value,
            is_active=True,
            is_parent_group=True,
            created_by=user,
            group_id=group_id,
            job_id=job.id,
            further_inspection=further_inspection,
            is_lock_closed=is_lock_closed,
        )
        job_log = JobLog.objects.create(
            job=job, created_by=request.user, status="Create", created_at=timezone.now()
        )
        # Send notification on Create Job if priority is on
        if "priority" in request.POST:
            members = (
                (transfer_job_obj.group.member.all())
                .exclude(role__title=UserRoleChoices.ADMIN.value)
                .exclude(role__title=UserRoleChoices.INSPECTOR.value)
            )
            CreateNotification(
                self,
                message=_("Job opened by @"),
                notification_type="Open",
                job_id=transfer_job_obj.id,
                members=members,
            )
        return JsonResponse({"create_job_status": "success"})


# job detail view
@method_decorator(login_required, name="dispatch")
class JobDetailView(TemplateView):
    model = TransferJob
    template_name = "job_detail.html"
    success_url = reverse_lazy("jobs:jobdetail")

    def get_context_data(self, **kwargs):
        pk = kwargs.get("pk")
        job_obj = get_object_or_404(TransferJob, id=pk)

        job_groups = (
            TransferJob.objects.filter(id=job_obj.id).order_by("-created_at").first()
        )
        return_job = ReturnJob.objects.filter(job_id=job_obj).first()
        if not return_job:
            return_job = ReturnJob.objects.filter(duplicate_id=job_obj).first()

        transferred_groups = TransferJob.objects.filter(job_id=job_obj.job_id).order_by(
            "created_at"
        )

        current_user = self.request.user
        context = super().get_context_data(
            **kwargs,
            job_obj=job_obj,
            group_list=list(
                Group.objects.filter(member=current_user.id).exclude(is_archive=True)
                if not current_user.is_superuser
                else (
                    Group.objects.exclude(is_archive=True)
                    if current_user.is_superuser
                    else None
                )
            ),
            # duplicate_job_list=duplicate_job_list(self),
            material_bills_list=material_bills_list(self),
            sign_bills_list=sign_bills_list(self),
            job_groups=job_groups,
            is_sign=is_sign(self, job_id=job_obj.id),
            type_counting_list=[val for key, val in TypeCounting.choices],
            type_list=[val for key, val in BillType.choices],
            return_job=return_job,
            close_job_bill=CloseJobBill.objects.filter(
                job_id__in=TransferJob.objects.filter(job_id=job_obj.job_id)
            ),
            transferred_group=transferred_groups,
            current_group=transferred_groups.filter(is_active=True).first(),
            google_api_key=settings.GOOGLE_API_KEY,
            roles=UserRole.objects.all(),
            permissions=Group.objects.all(),
            main_group=TransferJob.objects.filter(
                job_id=job_obj.job_id, is_parent_group=True
            ).first(),
            notification=NotificationList(self),
        )
        return context


# List for ReturnJob Module
@method_decorator(login_required, name="dispatch")
class ReturnJobListView(ListView):
    model = ReturnJob
    allowed_extensions = ["jpg", "jpeg", "png"]
    template_name = "return_job.html"
    first_job_image_subquery = (
        JobImage.objects.filter(job=OuterRef("job__job"))
        .annotate(
            file_extension=Case(
                *[
                    When(
                        image__iendswith=ext, then=Value(ext, output_field=CharField())
                    )
                    for ext in allowed_extensions
                ],
                default=Value(None, output_field=CharField()),
            )
        )
        .filter(file_extension__isnull=False)
        .order_by("id")
        .values("image")[:1]
    )
    queryset = (
        ReturnJob.objects.exclude(
            job__in=TransferJob.objects.filter(group__is_archive=True).values_list(
                "job_id", flat=True
            )
        )
        .select_related("job__job", "duplicate__job")
        .prefetch_related("job__job__job_image", "duplicate__job__job_image")
        .annotate(first_job_image=Subquery(first_job_image_subquery))
    )
    success_url = reverse_lazy("jobs:return-job-list")

    def get_queryset(self):
        search = self.request.GET.get("search")
        if search:
            return (
                super()
                .get_queryset()
                .filter(
                    Q(duplicate__job__address__icontains=search)
                    | Q(duplicate__job__address_information__icontains=search)
                    | Q(job__job__address__icontains=search)
                    | Q(job__job__address_information__icontains=search)
                    | Q(job__job__id__icontains=search)
                    | Q(job__job__duplicate_reference__icontains=search),
                )
            )
        return super().get_queryset()

    def get_context_data(self, **kwargs):
        queryset = self.object_list
        context = super().get_context_data(**kwargs)
        date_range = self.request.GET.get("date_range")
        date_list = date_range.split() if date_range else None
        from_date = date_list[0] if date_list else None
        to_date = date_list[2] if date_list and len(date_list) > 2 else from_date
        page_number = self.request.GET.get("page")
        current_user = self.request.user

        if date_range:
            return_job = (
                queryset.filter(
                    return_to=current_user.id,
                    status__in=[
                        str(JobStatus.WRONG_INFORMATION.value),
                        str(JobStatus.DUPLICATE.value),
                    ],
                    created_at__date__range=[from_date, to_date],
                ).order_by("-created_at")
                if not current_user.is_superuser
                else queryset.filter(
                    status__in=[
                        str(JobStatus.WRONG_INFORMATION.value),
                        str(JobStatus.DUPLICATE.value),
                    ],
                    created_at__date__range=[from_date, to_date],
                ).order_by("-created_at")
            )
        else:
            return_job = (
                queryset.filter(
                    return_to=current_user.id,
                    status__in=[
                        str(JobStatus.WRONG_INFORMATION.value),
                        str(JobStatus.DUPLICATE.value),
                    ],
                ).order_by("-created_at")
                if not current_user.is_superuser
                else queryset.filter(
                    status__in=[
                        str(JobStatus.WRONG_INFORMATION.value),
                        str(JobStatus.DUPLICATE.value),
                    ],
                ).order_by("-created_at")
            )

        paginator = Paginator(return_job, 10)
        context["paginator"] = paginator
        context["return_jobs"] = paginator.get_page(page_number)

        try:
            context["page_range"] = paginator.page(page_number)
        except PageNotAnInteger:
            context["page_range"] = paginator.page(1)
        except EmptyPage:
            context["page_range"] = paginator.page(paginator.num_pages)

        context["from_date"] = from_date
        context["to_date"] = to_date
        # context["duplicate_job_list"] = duplicate_job_list(self)
        context["material_bills_list"] = material_bills_list(self)
        context["sign_bills_list"] = sign_bills_list(self)
        context["google_api_key"] = settings.GOOGLE_API_KEY
        context["notification"] = NotificationList(self)
        return context


def get_job_log_data(job_logs, by_type):
    by_field = getattr(job_logs, f"{by_type}_by", None)
    if by_field:
        log_data = {
            "id": by_field.id,
            "user_name": by_field.user_name,
            "profile_image": (
                by_field.profile_image.url
                if by_field.profile_image
                else "/static/assets/img/avatars/avatar.svg"
            ),
            "email": by_field.email,
            "role": by_field.role.title,
            "phone": str(by_field.phone),
            "created_at": job_logs.created_at.date(),
            "status": job_logs.status,
            "label": "Job_" + by_type + "_by",
            "label2": "Job_" + by_type + "_on",
        }
        return log_data
    return None


# Get Data for Edit job
class EditJobDetail(ListView):
    model = TransferJob
    queryset = TransferJob.objects.exclude(group__is_archive=True)

    def get(self, request, *args, **kwargs):
        id = kwargs["pk"]
        user = request.user
        button_id = request.GET.get("button_id")
        user = request.user
        if button_id == "module":
            job_id = TransferJob.objects.filter(id=id).first()
            if job_id == None:
                return JsonResponse({"error": "This job is not available"})
            module_job_status = job_id.status
            job = TransferJob.objects.filter(
                job_id=job_id.job_id, is_parent_group=True
            ).first()
        else:
            job = self.queryset.filter(id=id).first()
        group = TransferJob.objects.filter(job__id=job.job.id, is_active=True).last()
        forms = job.group.form.all()
        is_sign_bill = list(is_sign(self, job_id=job.id))
        jobs = (
            TransferJob.objects.filter(job_id=job.job.id)
            .prefetch_related("job__job_image")
            .values_list("id", flat=True)
        )
        main_group = TransferJob.objects.get(job_id=job.job.id, is_parent_group=True)
        close_bills = CloseJobBill.objects.filter(job_id__in=Subquery(jobs)).values()

        image_with_id = [
            {"img": img_obj.image.url, "id": img_obj.id}
            for img_obj in job.job.job_image.all()
        ]
        attachment = [
            [
                os.path.splitext(attachment_obj.attachment.url)[1],
                os.path.basename(attachment_obj.attachment.url),
                os.path.isfile(attachment_obj.attachment.url),
                attachment_obj.id,
                attachment_obj.attachment.url,
                f"{round(float(attachment_obj.attachment.size / 1024 / 1024), 2)} MB",
            ]
            for attachment_obj in job.job.job_attachment.all()
        ]
        job_logs = JobLog.objects.filter(job__job_id=job.job.job_id).order_by(
            "created_at"
        )
        job_logs_data = []
        for status in job_logs:
            if status.status == "Create":
                log_data = get_job_log_data(status, "created")
            if status.status == "Update":
                log_data = get_job_log_data(status, "updated")
            if status.status == "Return":
                log_data = get_job_log_data(status, "returned")
            if status.status == "Close":
                log_data = get_job_log_data(status, "closed")
            if status.status == "Partial":
                log_data = get_job_log_data(status, "partially_closed")
            if status.status == "Transfer":
                log_data = get_job_log_data(status, "transferred")
            job_logs_data.append(log_data)

        job_data = {
            "id": job.id,
            "status": job.status,
            "job_id": job.job.id,
            "job_job_id": job.job.job_id,
            "job_status": job.status,
            "address": job.job.address,
            "address_information": job.job.address_information,
            "latitude": job.job.latitude,
            "longitude": job.job.longitude,
            "description": job.job.description,
            "group_id": job.group.id,
            "group": group.group.name,
            "main_group": main_group.group.id,
            "priority": job.job.priority,
            "further_inspection": job.job.further_inspection,
            "further_billing": job.further_billing,
            "attachment": attachment,
            "notes": list(
                job.job.job_notes.all().values(
                    "id",
                    "note",
                    "created_at",
                    "updated_at",
                    "created_by__user_name",
                    "updated_by__user_name",
                )
            ),
            "forms": list(forms.values()),
            "default_bills": (
                list(forms.first().bill.all().exclude(type="Sign").values())
                if forms.first()
                else None
            ),
            "is_sign": is_sign_bill,
            "job_close_bills": list(close_bills),
            "image_with_id": image_with_id,
            "job_reviewed": job.is_reviewed,
            "module_job_status": module_job_status if button_id == "module" else None,
            "created_at": job.job.created_at.date(),
            "is_active": job.is_active,
        }
        if job.job.created_by:
            job_data["created_by"] = {
                "id": job.job.created_by.id,
                "user_name": job.job.created_by.user_name,
                "profile_image": (
                    job.job.created_by.profile_image.url
                    if job.job.created_by.profile_image
                    else "/static/assets/img/avatars/avatar.svg"
                ),
                "email": job.job.created_by.email,
                "role": job.job.created_by.role.title,
                "phone": str(job.job.created_by.phone),
            }
        job_data["job_logs_data"] = job_logs_data
        if job.job.is_lock_closed and not (
            user.role.title == UserRoleChoices.ADMIN.value
            or user.is_superuser
            or job.job.created_by == user
        ):
            job_data["is_lock_closed"] = True
        return JsonResponse(job_data)


# Detail for ReturnJob Module
@method_decorator(login_required, name="dispatch")
class ReturnJobDetailView(ListView):
    model = ReturnJob
    template_name = "return_job.html"
    queryset = ReturnJob.objects.exclude(
        job__in=TransferJob.objects.filter(
            group__is_archive=True, is_active=True
        ).values_list("job_id", flat=True)
    )
    success_url = reverse_lazy("jobs:return-job-details")

    def get(self, request, *args, **kwargs):
        job_id = self.request.GET.get("job_id")
        job_status = self.request.GET.get("job_status")
        return_job_data = self.queryset.filter(id=job_id, status=job_status).first()
        image = [
            {"img_url": img_obj.image.url, "img_id": img_obj.id}
            for img_obj in return_job_data.job.job.job_image.all()
        ]
        attachement = [
            [
                os.path.splitext(attachment_obj.attachment.url)[1],
                os.path.basename(attachment_obj.attachment.url),
                os.path.isfile(attachment_obj.attachment.url),
                attachment_obj.id,
            ]
            for attachment_obj in return_job_data.job.job.job_attachment.all()
        ]
        job_logs = JobLog.objects.filter(
            job__job_id=return_job_data.job.job.job_id
        ).order_by("created_at")
        job_logs_data = []
        for status in job_logs:
            if status.status == "Create":
                log_data = get_job_log_data(status, "created")
            if status.status == "Update":
                log_data = get_job_log_data(status, "updated")
            if status.status == "Return":
                log_data = get_job_log_data(status, "returned")
            if status.status == "Close":
                log_data = get_job_log_data(status, "closed")
            if status.status == "Partial":
                log_data = get_job_log_data(status, "partially_closed")
            if status.status == "Transfer":
                log_data = get_job_log_data(status, "transferred")
            job_logs_data.append(log_data)

        return_job = {
            "id": return_job_data.id,
            "status": return_job_data.status,
            "job_id": return_job_data.job.job.id,
            "job_job_id": return_job_data.job.job.job_id,
            "job_status": return_job_data.job.status,
            "address": return_job_data.job.job.address,
            "address_information": return_job_data.job.job.address_information,
            "image": image,
            "latitude": return_job_data.job.job.latitude,
            "longitude": return_job_data.job.job.longitude,
            "notes": return_job_data.notes,
        }

        if return_job_data.duplicate:
            duplicate_image = [
                img_obj.image.url
                for img_obj in return_job_data.duplicate.job.job_image.all()
            ]
            duplicate_attachement = [
                os.path.splitext(attachment_obj.attachment.url)[1]
                for attachment_obj in return_job_data.duplicate.job.job_attachment.all()
            ]
            return_job_data = {
                "duplicate_job_id": return_job_data.duplicate.job.id,
                "duplicate_job_job_id": return_job_data.duplicate.job.job_id,
                "duplicate_status": return_job_data.duplicate.job.status,
                "duplicate_address": return_job_data.duplicate.job.address,
                "duplicate_address_information": return_job_data.duplicate.job.address_information,
                "duplicate_description": return_job_data.duplicate.job.description,
                "duplicata_image": duplicate_image,
                "duplicate_attachement": duplicate_attachement,
                "duplicate_priority": return_job_data.duplicate.job.priority,
                "duplicate_further_inspection": return_job_data.duplicate.further_inspection,
                "duplicate_latitude": return_job_data.duplicate.job.latitude,
                "duplicate_longitude": return_job_data.duplicate.job.longitude,
            }
        else:
            return_job_data = {
                "comment": return_job_data.comment,
                "job_status": return_job_data.job.status,
                "description": return_job_data.job.job.description,
                "attachement": attachement,
                "group": return_job_data.job.group.name,
                "priority": return_job_data.job.job.priority,
                "further_inspection": return_job_data.job.further_inspection,
                "job_logs_data": job_logs_data,
            }
        return_job.update(return_job_data)
        return JsonResponse({"data": return_job})


# Create for ReturnJob Module
@method_decorator(login_required, name="dispatch")
class ReturnJobCreateView(CreateView):
    model = ReturnJob
    template_name = "return_job.html"
    form_class = ReturnJobForm

    def post(self, request, *args, **kwargs):
        data = request.POST.copy()
        job_obj = kwargs.get("pk")
        current_user = request.user
        # if requested job is not transfered then raise error
        transfer_job = TransferJob.objects.filter(id=job_obj, is_active=True).first()
        if not transfer_job:
            # {"transfer_error": "Job transfer required."}
            return JsonResponse({"transfer_error": "יש להעביר את המשימה"})

        # get main group's member role
        main_group_tarnsfer_job = TransferJob.objects.filter(
            job_id=transfer_job.job_id, is_parent_group=True
        ).first()
        parent_group = Group.objects.filter(id=main_group_tarnsfer_job.group_id)
        inspector_list = parent_group.filter(
            member__role__title=UserRoleChoices.INSPECTOR.value
        ).values_list("member", flat=True)

        # # if Any inspectors are not in main group then raise error
        # if not inspector_list:
        #     return JsonResponse(
        #         {
        #             "error": _(
        #                 "There is no any inspector. Please add at least one inspector to main group."
        #             )
        #         }
        #     )

        # check if requested job is returned with selected job then raise error
        response = None
        if "comment" not in data:
            original_id = data.getlist("job")[-1]
            original_job = get_object_or_404(TransferJob, id=original_id)
            instance = ReturnJob.objects.filter(
                Q(job__id=original_id), Q(duplicate__id=job_obj)
            ).first()
            response = (
                {
                    "original_job": model_to_dict(
                        instance=original_job.job,
                        fields=[
                            "id",
                            "address",
                            "address_information",
                            "latitude",
                            "longitude",
                        ],
                    ),
                    "original_job_image": original_job.job.job_image.first().image.url,
                }
                if original_job.job.job_image.first()
                else {
                    "original_job": model_to_dict(
                        instance=original_job.job,
                        fields=[
                            "id",
                            "address",
                            "address_information",
                            "latitude",
                            "longitude",
                        ],
                    )
                }
            )
            if instance:
                return JsonResponse(
                    {
                        "error": _(
                            "Job is already returned with this duplicate job to all inspectors of main Group."
                        )
                    }
                )

        # Create Retutn job and update status in Transfer job model
        if "comment" in data:
            data.update(
                {
                    "job": str(main_group_tarnsfer_job.id),
                    "status": JobStatus.WRONG_INFORMATION.value,
                    "group": str(transfer_job.group_id),
                }
            )
        else:
            data.update(
                {
                    "duplicate": str(job_obj),
                    "status": JobStatus.DUPLICATE.value,
                    "group": str(transfer_job.group_id),
                }
            )

        form = self.form_class(data)
        if form.is_valid():
            returned_job = form.save()
            returned_job.created_by = current_user
            returned_job.return_to.set([*list(inspector_list)])
            returned_job.save()

            # Send notification on Transfer Job
            members = (
                (transfer_job.group.member.all())
                .exclude(role__title=UserRoleChoices.ADMIN.value)
                .exclude(role__title=UserRoleChoices.GROUP_MANAGER.value)
            )

            # Update Transfer job instance of requested returned job.
            main_group_tarnsfer_job.status = JobStatus.RETURN.value
            main_group_tarnsfer_job.job.status = JobStatus.RETURN.value
            main_group_tarnsfer_job.updated_by = current_user
            main_group_tarnsfer_job.is_active = True
            main_group_tarnsfer_job.save()
            main_group_tarnsfer_job.job.save()

            transfer_job.status = JobStatus.RETURN.value
            transfer_job.is_active = (
                False if transfer_job.is_parent_group != True else True
            )
            transfer_job.save()
            job_log = JobLog.objects.create(
                job=transfer_job.job,
                returned_by=current_user,
                status="Return",
                created_at=timezone.now(),
            )
            # Send Return job Notification
            CreateNotification(
                self,
                message=_("Job is Returned by @"),
                notification_type="Return",
                job_id=transfer_job.id,
                members=members,
            )
            return JsonResponse(
                {"job_return_status": response if "duplicate" in data else "success"}
            )

        return JsonResponse({"error": form.errors})


# Update for ReturnJob Module
@method_decorator(login_required, name="dispatch")
class ReturnJobUpdateView(UpdateView):
    model = ReturnJob
    template_name = "return_job.html"
    success_url = reverse_lazy("jobs:return-job-list")

    def post(self, request, *args, **kwargs):
        form_data = request.POST
        files_data = self.request.FILES.getlist("attachment")
        user = self.request.user
        data = request.GET.get("data")
        return_job = ReturnJob.objects.get(id=form_data["id"])
        job_log = JobLog.objects.create(
            job=return_job.job.job,
            updated_by=request.user,
            status="Update",
            created_at=timezone.now(),
        )
        if data == JobStatus.DUPLICATE.value:
            original_job_id = return_job.job_id
            duplicate_job_id = return_job.duplicate_id

            original_job = TransferJob.objects.filter(id=original_job_id).first()
            transfer_job_obj = get_object_or_404(TransferJob, id=duplicate_job_id)

            reference_job = original_job.job.get_duplicate_reference()
            reference_job = (
                ", " + transfer_job_obj.job.job_id
                if reference_job
                else transfer_job_obj.job.job_id
            )

            original_job.job.set_duplicate_reference(reference_job)
            original_job.job.save()

            # delete duplicate job
            duplicate_obj = get_object_or_404(ReturnJob, duplicate_id=duplicate_job_id)
            duplicate_obj.delete()

            # delete transfer job
            transfer_job_obj.delete()
            transfer_job_obj.job.delete()

            return JsonResponse({"status": _("successfully Deleted")})

        if data == "wrong_save":
            # Update Job object
            Job.objects.filter(id=form_data["transfer_job_id"]).update(
                address=form_data["address"],
                address_information=form_data["address_info"],
                description=form_data["description"],
                updated_by=user,
                priority=True if "priority" in form_data else False,
                latitude=form_data["latitude"],
                longitude=form_data["longitude"],
            )

            # Update TransferJob Object
            main_group_job = TransferJob.objects.filter(
                id=return_job.job_id, is_parent_group=True
            ).first()
            main_group_job.status = JobStatus.OPEN.value
            main_group_job.job.status = JobStatus.OPEN.value
            main_group_job.is_active = False
            main_group_job.updated_by = user
            main_group_job.further_inspection = (
                True if "further_inspection" in form_data else False
            )
            main_group_job.save()
            main_group_job.job.save()

            transfer_obj = TransferJob.objects.filter(
                job_id=(return_job.job).job_id,
                group_id=return_job.group_id,
            ).first()

            transfer_obj.status = JobStatus.OPEN.value
            transfer_obj.is_active = True
            transfer_obj.updated_by = user
            transfer_obj.further_inspection = (
                True if "further_inspection" in form_data else False
            )
            transfer_obj.save()

            # Bulk create attechment
            if files_data:
                image_obj = []
                attechment_obj = []
                for attechment in files_data:
                    if attechment.name.split(".")[-1] in imageVideoExtensions:
                        image_obj.append(
                            JobImage(
                                image=attechment,
                                job_id=(
                                    form_data["transfer_job_id"]
                                    if "transfer_job_id" in form_data
                                    else form_data["duplicate_id"]
                                ),
                                created_by=user,
                                updated_by=user,
                            )
                        )
                    else:
                        attechment_obj.append(
                            JobAttachment(
                                attachment=attechment,
                                job_id=(
                                    form_data["transfer_job_id"]
                                    if "transfer_job_id" in form_data
                                    else form_data["duplicate_id"]
                                ),
                                created_by=user,
                                updated_by=user,
                            )
                        )
                JobAttachment.objects.bulk_create(attechment_obj)
                JobImage.objects.bulk_create(image_obj)

            delete_image_id = form_data.get("image_delete")
            delete_docs_id = form_data.get("docs_delete")
            if delete_docs_id or delete_image_id:
                delete_attachment(delete_docs_id, delete_image_id)

            # Delete ReturnJob Object
            ReturnJob.objects.get(id=form_data["id"]).delete()

            # Send notification on Wrong info job
            members = (transfer_obj.group.member.all()).exclude(
                role__title=UserRoleChoices.INSPECTOR.value
            )
            CreateNotification(
                self,
                message=_("Job is Updated by @"),
                notification_type="Open",
                job_id=transfer_obj.id,
                members=members,
            )
            return JsonResponse({"status": _("successfully Updated")})

        elif data == "duplicate_save":
            transfer_obj = TransferJob.objects.filter(
                job_id=return_job.duplicate.job_id, group_id=return_job.group_id
            ).first()
            transfer_obj.status = JobStatus.OPEN.value
            transfer_obj.job.status = JobStatus.OPEN.value
            transfer_obj.is_active = True
            transfer_obj.save()
            transfer_obj.job.save()
            ReturnJob.objects.get(id=form_data["id"]).delete()
            return JsonResponse({"status": _("successfully Updated")})


# Delete for ReturnJob Module
@method_decorator(login_required, name="dispatch")
class ReturnJobDeleteView(DeleteView):
    model = Job
    template_name = "return_job.html"
    success_url = reverse_lazy("jobs:return-job-list")


# Jobs for Map Module
@method_decorator(login_required, name="dispatch")
class RecentSearchJob(ListView):
    model = RecentSearchJob
    template_name = "map.html"
    success_url = reverse_lazy("jobs:recent-search-job")

    def get_queryset(self):
        user = self.request.user
        queryset = TransferJob.objects.filter(
            is_active=True, status__in=[JobStatus.OPEN.value, JobStatus.TRANSFER.value]
        )

        if not user.is_superuser:
            queryset = queryset.filter(group__member=user.id)

            first_group_id = (
                Group.objects.filter(member=user.id)
                .exclude(is_archive=True)
                .values_list("id", flat=True)
                .first()
            )

        first_group_id = (
            Group.objects.exclude(is_archive=True).values_list("id", flat=True).first()
        )

        # Retrieve the previous group_id from session
        previous_group_id = self.request.session.get("previous_group_id")
        if previous_group_id:
            queryset = queryset.filter(group__id=previous_group_id)
        else:
            queryset = queryset.filter(group__id=first_group_id)
        return queryset

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        queryset = self.object_list
        current_user = self.request.user
        context["google_api_key"] = settings.GOOGLE_API_KEY

        if not current_user.is_superuser:
            context["groups"] = Group.objects.filter(member=current_user.id).exclude(
                is_archive=True
            )
        else:
            context["groups"] = Group.objects.exclude(is_archive=True)

        job_list = queryset.order_by("-created_at")
        context["job_list"] = job_list.annotate(
            job_image=Subquery(first_image_subquery("job"))
        ).prefetch_related("job", "job__job_image")

        context["notification"] = NotificationList(self)
        context["group_list"] = context["groups"]
        # context["duplicate_job_list"] = job_list

        # Retrieve the previous group_id from session
        previous_group_id = self.request.session.get("previous_group_id")
        if previous_group_id:
            context["previous_group_id"] = previous_group_id

        return context


# Group wise Jobs for Map Module
def get_jobs_for_group(request, group_id):
    user = request.user
    if group_id:
        request.session["previous_group_id"] = group_id

    job_values = [
        "id",
        "job__id",
        "job__address",
        "status",
        "created_at",
        "job__latitude",
        "job__longitude",
        "job__description",
        "job__priority",
    ]
    # Get the sorting order from the request data
    sort_order = request.POST.get("sort_order")
    order = "-created_at"
    if sort_order == "ascending":
        order = "created_at"

    if group_id == 0:
        jobs = TransferJob.objects.order_by(order).filter(
            is_active=True,
            status__in=[JobStatus.OPEN.value, JobStatus.TRANSFER.value],
        )
        jobs_images = JobImage.objects.all().values(
            "job__id",
            "image",
        )
    else:
        jobs = TransferJob.objects.order_by(order).filter(
            group=group_id,
            is_active=True,
            status__in=[JobStatus.OPEN.value, JobStatus.TRANSFER.value],
        )
        jobs_images = JobImage.objects.filter(job__assign_job__group=group_id).values(
            "job__id",
            "image",
        )

    if not user.is_superuser:
        jobs = jobs.filter(group__member=user.id)

    jobs = jobs.values(*job_values)
    job_list = [job for job in jobs]
    response = {"job_list": job_list, "job_images": list(jobs_images)}
    return JsonResponse(response, safe=False)


# Create PDF for ReportGenerator Module
@method_decorator(login_required, name="dispatch")
class GeneratePdf(View):
    model = Job
    template_name = "web_report.html"
    queryset = TransferJob.objects.filter(
        status=JobStatus.CLOSE.value, group__is_archive=False, is_active=True
    )
    success_url = reverse_lazy("jobs:report-generate")
    URL = os.environ["URL"]

    def get(self, request, *args, **kwargs):
        start_time = time.time()
        request_id = request.META.get("HTTP_X_REQUEST_ID", "N/A")

        single_report = self.request.GET.get("single_report")
        single_job = self.request.GET.get("job", None)
        date_range = self.request.GET.get("date_range")
        report = self.request.GET.get("report")
        with_image = self.request.GET.get("with_image")
        date_list = date_range.split() if date_range else None
        from_date = date_list[0] if date_list else datetime.today().strftime("%Y-%m-%d")
        to_date = (
            from_date
            if not date_list
            else date_list[2] if len(date_list) > 1 else from_date
        )
        from_date_obj = datetime.strptime(from_date, "%Y-%m-%d")
        to_date_obj = datetime.strptime(to_date, "%Y-%m-%d")

        date_data = {
            "from_date": from_date_obj.strftime("%d-%m-%Y"),
            "to_date": to_date_obj.strftime("%d-%m-%Y"),
        }

        groups_value = self.request.GET.get("groups")
        groups = groups_value.split("|") if groups_value else None

        if groups:
            jobs_by_group = self.queryset.filter(group__name__in=groups)
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
            groups = list(
                self.queryset.distinct().values_list("group__name", flat=True)
            )

        queryset = (
            TransferJob.objects.filter(id=single_job)
            if single_report == "True"
            else self.queryset.filter(id__in=tranfer_job_id_list)
        )

        if single_report == "True":
            instances = queryset
        else:
            if from_date and to_date:
                # Include jobs with NULL closed_at OR jobs within date range
                instances = queryset.filter(
                    Q(job__closed_at__isnull=True)
                    | Q(
                        job__closed_at__date__gte=from_date,
                        job__closed_at__date__lte=to_date,
                    )
                ).order_by("job__closed_at")
            elif from_date:
                # Include jobs with NULL closed_at OR jobs after from_date
                instances = queryset.filter(
                    Q(job__closed_at__isnull=True)
                    | Q(job__closed_at__date__gte=from_date)
                ).order_by("job__closed_at")
            else:
                # No date filter - return all jobs in queryset
                instances = queryset.order_by("job__closed_at")

        query_time = time.time() - start_time
        job_count = (
            len(instances) if hasattr(instances, "__len__") else instances.count()
        )
        logger.info(
            f"[{request_id}] GeneratePdf report={report}: Query completed in {query_time:.2f}s, {job_count} jobs"
        )
        logger.info(
            f"[{request_id}] DEBUG: from_date={from_date}, to_date={to_date}, groups={groups}, single_report={single_report}"
        )

        if report == "detail" or single_report == "True":
            # Prefetch all bills once for better performance
            all_job_ids = [instance.job_id for instance in instances]
            print(f"DEBUG GeneratePdf: Fetching bills for {len(all_job_ids)} job_ids: {all_job_ids[:5]}...", flush=True)
            logger.info(
                f"[{request_id}] Fetching bills for {len(all_job_ids)} jobs: {all_job_ids[:5]}..."
            )

            all_bills = (
                CloseJobBill.objects.filter(job__job_id__in=all_job_ids)
                .select_related("job")
                .values("id", "name", "type", "measurement", "type_counting", "image", "job_id", "job__job_id")
            )
            print(f"DEBUG GeneratePdf: Found {len(all_bills)} total bills", flush=True)

            bill_time = time.time() - start_time - query_time
            logger.info(
                f"[{request_id}] Bills fetched in {bill_time:.2f}s, {len(all_bills)} bills"
            )

            # Build lookup dictionary for bills by Job ID (not TransferJob ID)
            # This matches the preview logic where all TransferJobs for the same Job see the same bills
            bills_by_job_id = {}
            for bill_data in all_bills:
                # job__job_id gives us the actual Job ID (bill belongs to TransferJob, TransferJob belongs to Job)
                job_id = bill_data["job__job_id"]
                if job_id not in bills_by_job_id:
                    bills_by_job_id[job_id] = []
                bills_by_job_id[job_id].append(bill_data)
            
            print(f"DEBUG GeneratePdf: Grouped bills by Job ID: {list(bills_by_job_id.keys())[:10]}", flush=True)

            data = []
            for instance in instances:
                logger.info(
                    f"[{request_id}] Processing TransferJob {instance.id} (Job {instance.job_id}), closed_at={instance.job.closed_at}"
                )
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

                    new_dict["notes"] = instance.job.job_notes.all()
                    new_dict["description"] = instance.job.description
                new_dict["updated_at"] = (
                    instance.job.closed_at.date() if instance.job.closed_at else ""
                )
                new_dict["created_by"] = (
                    instance.job.created_by.user_name
                    if instance.job.created_by
                    else None
                )
                new_dict["created_at"] = str(instance.job.created_at.date())

                # Use bills grouped by Job ID (not TransferJob ID) to match preview behavior
                bill_data_list = bills_by_job_id.get(instance.job_id, [])
                print(f"DEBUG GeneratePdf: TransferJob {instance.id} (Job {instance.job_id}): Found {len(bill_data_list)} bills", flush=True)
                logger.info(
                    f"[{request_id}] TransferJob {instance.id}: Found {len(bill_data_list)} bills in lookup"
                )

                if with_image == "true" or single_report == "True":
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

                sign_bills_list = []
                detail_bills_list = []

                for bill_data in bill_data_list:
                    print(f"DEBUG GeneratePdf: Bill {bill_data['name']}, type={bill_data['type']}, measurement={bill_data['measurement']}", flush=True)
                    bill_dict = {}
                    if bill_data["type"] == "Sign" and bill_data["measurement"] is not None:
                        bill_dict.update(
                            {
                                "bill_name": bill_data["name"],
                                "bill_unit": bill_data["type_counting"],
                                "quantity": round(bill_data["measurement"], 2),
                                "image": (
                                    f"{self.URL}{bill_data['image']}"
                                    if bill_data["image"]
                                    else request.build_absolute_uri("/")
                                    + "static/assets/img/bill.svg"
                                ),
                            }
                        )
                        sign_bills_list.append(bill_dict)
                    elif bill_data["type"] == "Material" and bill_data["measurement"] is not None:
                        bill_dict.update(
                            {
                                "bill_name": bill_data["name"],
                                "bill_unit": bill_data["type_counting"],
                                "quantity": round(bill_data["measurement"], 2),
                            }
                        )
                        detail_bills_list.append(bill_dict)

                if sign_bills_list:
                    new_dict["sign_bills"] = sign_bills_list
                    logger.info(
                        f"[{request_id}] TransferJob {instance.id} has {len(sign_bills_list)} sign bills"
                    )
                if detail_bills_list:
                    new_dict["detail_bills"] = detail_bills_list
                    logger.info(
                        f"[{request_id}] TransferJob {instance.id} has {len(detail_bills_list)} detail bills"
                    )

                if not sign_bills_list and not detail_bills_list:
                    logger.info(
                        f"[{request_id}] TransferJob {instance.id} has NO bills! bill_data_list length: {len(bill_data_list)}"
                    )
                    new_dict["detail_bills"] = detail_bills_list
                data.append(new_dict)

            logger.info(f"[{request_id}] Total jobs added to PDF data: {len(data)}")

            data = {
                "context": data,
                "date": date_data,
                "url": self.URL,
                "single_report": single_report,
                "groups": groups[0],
            }

            file_name = "Detail-report"
            pdf = generate_pdf("web_report.html", self.request, data, file_name)
            path = os.path.join(settings.BASE_DIR, "media")

            total_time = time.time() - start_time
            logger.info(
                f"[{request_id}] Detail report PDF generated: {total_time:.2f}s total"
            )

            response = FileResponse(
                open(f"{path}/{file_name}.pdf", "rb"),
                content_type="application/pdf",
            )
            return response

        elif report == "sum_up":
            # Prefetch all bills once for better performance
            all_job_ids = list(instances.values_list("job_id", flat=True))
            all_bills = CloseJobBill.objects.filter(
                job__job_id__in=all_job_ids
            ).select_related("job")

            bill_time = time.time() - start_time - query_time
            logger.info(
                f"[{request_id}] Sum-up bills fetched in {bill_time:.2f}s, {len(all_bills)} bills"
            )

            # Calculate aggregates for all bills at once using defaultdict
            from collections import defaultdict

            # Aggregate sign bills by (type, type_counting)
            sign_aggregates = defaultdict(float)
            for bill in all_bills:
                if bill.type == "Sign" and bill.measurement is not None:
                    key = (bill.type, bill.type_counting)
                    sign_aggregates[key] += bill.measurement

            # Aggregate material bills by (name, type_counting)
            material_aggregates = defaultdict(float)
            for bill in all_bills:
                if bill.type == "Material" and bill.measurement is not None:
                    key = (bill.name, bill.type_counting)
                    material_aggregates[key] += bill.measurement

            group_wise_job_bill_list = []
            job_data_list = []

            for group in groups:
                group_jobs = instances.filter(group__name=group)
                group_job_ids = set(group_jobs.values_list("job_id", flat=True))

                for job in group_jobs:
                    if job.further_billing:
                        job_data = {
                            "address": job.job.address,
                            "job_id": job.job.job_id,
                            "job_group": job.group.name,
                            "notes": job.job.job_notes.all(),
                            "closed_date": job.job.closed_at,
                            "further_billing": job.job.further_billing,
                        }
                        job_data_list.append(job_data)

                # Filter bills for this group in memory
                bill_data_list_filtered = [
                    b for b in all_bills if b.job.job_id in group_job_ids
                ]
                # Filter bills for this group in memory
                bill_data_list_filtered = [
                    b for b in all_bills if b.job.job_id in group_job_ids
                ]

                bills = []
                list_of_sign_bills = []
                list_of_material_bills = []

                for bill in bill_data_list_filtered:
                    bills.append(
                        {
                            "name": bill.name,
                            "type": bill.type,
                            "type_counting": bill.type_counting,
                            "image": (
                                f"{self.URL}{bill.image}"
                                if bill.image
                                else (
                                    self.request.build_absolute_uri("/")
                                    + "static/assets/img/bill.svg"
                                    if bill.type == "Sign"
                                    else None
                                )
                            ),
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

                group_wise_job_bill_list.append(
                    {
                        "group_name": job.group.name,
                        "material": list_of_material_bills,
                        "sign_bill": list_of_sign_bills,
                    }
                )

            data = {
                "context": group_wise_job_bill_list,
                "date": date_data,
                "jobs": job_data_list,
            }

            file_name = "Sum-up-report"
            pdf = generate_pdf("web_sum_up_report.html", self.request, data, file_name)
            path = os.path.join(settings.BASE_DIR, "media")

            total_time = time.time() - start_time
            logger.info(
                f"[{request_id}] Sum-up report PDF generated: {total_time:.2f}s total"
            )

            response = FileResponse(
                open(f"{path}/{file_name}.pdf", "rb"),
                content_type="application/pdf",
            )
            return response


# PDF Generator for ReportGenerator Module
def generate_pdf(template_path, request, context, file_name):
    template = get_template(template_path)
    context.update({"request": request})
    html = template.render(context)

    path = os.path.join(settings.BASE_DIR, "media")
    pdf_file = f"{path}/{file_name}.pdf"
    pdf_bytes = HTML(string=html).write_pdf(pdf_file)

    response = HttpResponse(pdf_bytes, content_type="application/pdf")
    response["Content-Disposition"] = f'attachment; filename="{pdf_file}.pdf"'
    return response


# ReportGenerator Module
@method_decorator(login_required, name="dispatch")
class ReportGeneratorListView(ListView):
    model = Job
    template_name = "report_generator.html"
    queryset = TransferJob.objects.filter(
        status=JobStatus.CLOSE.value, group__is_archive=False, is_active=True
    )
    success_url = reverse_lazy("jobs:report-generate")
    URL = os.environ["URL"]

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        date_range = self.request.GET.get("date_range")
        date_list = date_range.split() if date_range else None
        from_date = (
            date_list[0]
            if date_list
            else datetime.today().replace(day=1).strftime("%Y-%m-%d")
        )
        to_date = (
            datetime.today().strftime("%Y-%m-%d")
            if not date_list
            else date_list[2] if len(date_list) > 1 else from_date
        )

        current_user = self.request.user
        context["group_list"] = (
            Group.objects.filter(member=current_user.id).exclude(is_archive=True)
            if not current_user.is_superuser
            else (
                Group.objects.exclude(is_archive=True)
                if current_user.is_superuser
                else None
            )
        )
        groups_value = self.request.GET.get("groups")
        get_groups = groups_value.split("|") if groups_value else None
        groups = get_groups if get_groups else [context["group_list"].first()]

        if groups:
            jobs_by_group = self.queryset.filter(group__name__in=groups)
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
            # Include jobs with NULL closed_at OR jobs within date range
            instances = queryset.filter(
                Q(job__closed_at__isnull=True)
                | Q(
                    job__closed_at__date__gte=from_date,
                    job__closed_at__date__lte=to_date,
                )
            ).order_by("job__closed_at")
        elif from_date:
            # Include jobs with NULL closed_at OR jobs after from_date
            instances = queryset.filter(
                Q(job__closed_at__isnull=True) | Q(job__closed_at__date__gte=from_date)
            ).order_by("job__closed_at")
        else:
            # No date filter - return all jobs in queryset
            instances = queryset.order_by("job__closed_at")

        # detail report generator
        detail_bills = []
        for instance in instances:
            if instance.job.closed_by and instance.job.closed_by.user_name:
                closed_by = instance.job.closed_by.user_name
            else:
                closed_by = instance.job.closed_by

            data = {
                "id": instance.id,
                "address": instance.job.address,
                "address_information": instance.job.address_information,
                "close_date": instance.job.closed_at,
                "closed_by": closed_by,
                "priority": instance.job.priority,
            }
            job_bills = CloseJobBill.objects.filter(job__job_id=instance.job_id)
            list_of_bills = []
            for bill in job_bills:
                if bill.measurement is not None:
                    bill_dict = {
                        "bill_name": bill.name,
                        "QTY": bill.measurement,
                        "type_counting": bill.type_counting,
                    }
                    list_of_bills.append(bill_dict)
            data.update({"bills": list_of_bills})
            detail_bills.append(data)

        # sum_up report generator
        instances = instances.values_list("job_id", flat=True)
        bill_data_list = CloseJobBill.objects.filter(job__job_id__in=instances)

        bills = []
        list_of_signs = []
        list_of_materials = []

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

        # Get unique data from bills(list)
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
                    F("measurement"),
                    output_field=FloatField(),
                )
            )
            if data["type_counting"] == TypeCounting.SQM.value:
                data["name"] = "תמרורים לפי מ״ר (439)"
            if data["type_counting"] == TypeCounting.UNITS.value:
                data["name"] = "תמרורים"
            data["quantity"] = bill_quantity["QTY"]
            list_of_signs.append(data)

        for data in unique_material:
            bill_quantity = bill_data_list.filter(
                name=data["name"],
                type_counting=data["type_counting"],
            ).aggregate(
                QTY=Sum(
                    F("measurement"),
                    output_field=FloatField(),
                )
            )

            data["quantity"] = bill_quantity["QTY"]
            list_of_materials.append(data)

        context["detail_bills"] = detail_bills
        context["sum_up_bills"] = list_of_signs + list_of_materials
        context["from_date"] = from_date
        context["to_date"] = to_date
        context["selected_groups"] = groups
        context["notification"] = NotificationList(self)
        return context


# TransferJob Module
@method_decorator(login_required, name="dispatch")
class TransferJobView(CreateView):
    model = TransferJob
    form_class = TransferJobForm
    template_name = "job_detail.html"

    def post(self, request, *args, **kwargs):
        data = request.POST.copy()
        job = TransferJob.objects.filter(id=kwargs.get("pk")).first()
        current_user = self.request.user

        if TransferJob.objects.filter(group=data["group"], job=job.job_id):
            job_transferred_groups = TransferJob.objects.filter(job=job.job_id)
            bulk_updated_fields = []
            for job_transferred in job_transferred_groups:
                job_transferred.status = JobStatus.TRANSFER.value
                job_transferred.is_active = False
                bulk_updated_fields.append(job_transferred)

            TransferJob.objects.bulk_update(
                bulk_updated_fields, ["status", "is_active"]
            )
            TransferJob.objects.filter(group=data["group"], job=job.job_id).update(
                status=JobStatus.OPEN.value, is_active=True
            )
            job_log = JobLog.objects.create(
                job=job.job,
                transferred_by=current_user,
                status="Transfer",
                created_at=timezone.now(),
            )
            return JsonResponse({"job_transfer_status": "success"})

        job.status = JobStatus.TRANSFER.value
        job.job.status = JobStatus.TRANSFER.value
        job.is_active = False
        job.save()
        job.job.save()
        data.update(
            {
                "job": job.job_id,
                "status": JobStatus.OPEN.value,
                "created_by": current_user.id,
                "updated_by": current_user.id,
                "is_active": True,
            }
        )

        form = self.form_class(data)
        if form.is_valid():
            form = form.save()

            job = Job.objects.get(id=form.job_id)
            job.updated_by = current_user
            job_log = JobLog.objects.create(
                job=job,
                transferred_by=current_user,
                status="Transfer",
                created_at=timezone.now(),
            )
            job.save()

            # Send notification on Transfer Job
            members = (
                (form.group.member.all())
                .exclude(role__title=UserRoleChoices.ADMIN.value)
                .exclude(role__title=UserRoleChoices.INSPECTOR.value)
            )
            CreateNotification(
                self,
                message=_("Job is transferred by @"),
                notification_type="Transfer",
                job_id=form.id,
                members=members,
            )
            return JsonResponse({"job_transfer_status": "success"})
        return JsonResponse({"error": form.errors})


# Add Sign Bills on close job
@method_decorator(login_required, name="dispatch")
class CloseJobBillView(CreateView):
    model = Bill
    template_name = "job_detail.html"
    success_url = reverse_lazy("jobs:jobdetail")

    def post(self, request, *args, **kwargs):
        job_id = request.GET.get("job_id")
        bills_id = self.request.POST.getlist("bills")
        bills = Bill.objects.filter(id__in=bills_id).values()
        data = {"bills": list(bills), "job_id": job_id}
        return JsonResponse(data)


# Add Forms on close job
@method_decorator(login_required, name="dispatch")
class CloseJobFormView(CreateView):
    model = Form
    template_name = "job_detail.html"
    success_url = reverse_lazy("jobs:jobdetail")

    def post(self, request, *args, **kwargs):
        job_id = request.GET.get("job_id")
        forms_id = self.request.POST.getlist("form")
        forms = (
            Form.objects.filter(id__in=forms_id)
            .values_list("bill", flat=True)
            .distinct()
        )
        forms_bills = Bill.objects.filter(
            id__in=forms, type=BillType.MATERIAL.value
        ).values()
        data = {"forms_bills": list(forms_bills), "job_id": job_id}
        return JsonResponse(data)


# Create specific job's Bill on close job
@method_decorator(login_required, name="dispatch")
class CreateCloseJobBill(CreateView):
    model = CloseJobBill
    form_class = CloseBillForm
    template_name = "job_detail.html"
    success_url = reverse_lazy("jobs:add-new-job")

    def form_valid(self, form):
        current_user = self.request.user
        form.instance.created_by = current_user
        form.instance.updated_by = current_user
        form.instance.is_close_time_created = True
        super().form_valid(form)
        bill_obj = CloseJobBill.objects.filter(id=form.instance.id).first()
        image = bill_obj.image.url if bill_obj.image else None
        job_bill = {
            "id": bill_obj.id,
            "name": bill_obj.name,
            "type_counting": bill_obj.type_counting,
            "jumping_ration": bill_obj.jumping_ration,
            "image": image,
            "type": bill_obj.type,
            "measurement": bill_obj.measurement,
            "is_created": bill_obj.is_created,
        }
        close_job_bill = {"close_job_bill": job_bill}
        return JsonResponse(close_job_bill)

    def post(self, request, *args, **kwargs):
        form = self.get_form()
        if form.is_valid():
            return self.form_valid(form)
        return JsonResponse({"error": form.errors if form.errors else None})


# Job Close view
@method_decorator(login_required, name="dispatch")
class JobCloseView(UpdateView):
    model = CloseJobBill
    template_name = "job_detail.html"

    def post(self, request, *args, **kwargs):
        data = request.POST
        main_group_id = data.get("main_group")

        status = request.GET.get("status")
        transfer_job_id = request.GET.get("transfer_job")
        update_status = request.GET.get("update")
        form_data = request.POST.getlist("tabledata")
        user = self.request.user
        delete_image_id = data.get("image_delete")
        delete_docs_id = data.get("docs_delete")
        further_billing = True if "further_billing" in data else False
        bulk_create_list = []
        bulk_update_list = []

        transfer_job = TransferJob.objects.get(id=transfer_job_id)
        main_group_job = TransferJob.objects.get(
            job_id=transfer_job.job_id, is_parent_group=True
        )
        job_all_groups = TransferJob.objects.filter(
            job_id=transfer_job.job_id
        ).values_list("group_id", flat=True)
        job = main_group_job.job

        # Bulk Create JobImage object
        if status in [JobStatus.CLOSE.value, JobStatus.PARTIAL.value]:
            close_job_image = True
        else:
            close_job_image = False

        images = []
        attachments = []
        notes = []

        for note in request.POST.getlist("note"):
            notes.append(note)

        if notes:
            for note in notes:
                JobNote.objects.create(
                    job=job,
                    note=note,
                    created_by=user,
                    updated_by=user,
                )

        updated_notes = request.POST.get("updated_notes")
        if updated_notes:
            updated_notes_dict = json.loads(updated_notes)
            for key, value in updated_notes_dict.items():
                id = int(key)
                note = value
                job_note = JobNote.objects.filter(id=id).first()
                job_note.note = note
                job_note.created_by = user
                job_note.save()

        for file in request.FILES.getlist("attachment"):
            if file.name.split(".")[-1] in imageVideoExtensions:
                images.append(file)
            else:
                attachments.append(file)

        if images:
            for image in images:
                JobImage.objects.create(
                    job=job,
                    image=image,
                    created_by=user,
                    updated_by=user,
                    close_job_image=close_job_image,
                )

        if attachments:
            for attachment in attachments:
                JobAttachment.objects.create(
                    job=job,
                    attachment=attachment,
                    created_by=user,
                    updated_by=user,
                    close_job_attachment=close_job_image,
                )

        if status in [JobStatus.OPEN.value, JobStatus.TRANSFER.value]:
            open_main_group_job = TransferJob.objects.get(
                job_id=transfer_job.job_id, is_parent_group=True
            )
            transfer_job.further_billing = (
                True if data.get("further_billing") == "on" else False
            )

            transfer_job.further_inspection = (
                True if data.get("further_inspection") == "on" else False
            )
            transfer_job.updated_by = user
            transfer_job.save()

            if status == JobStatus.OPEN.value:
                job_log = JobLog.objects.create(
                    job=transfer_job.job,
                    updated_by=user,
                    status="Update",
                    created_at=timezone.now(),
                )
            else:
                job_log = JobLog.objects.create(
                    job=transfer_job.job,
                    transferred_by=user,
                    status="Transfer",
                    created_at=timezone.now(),
                )
            if job.job_id != data.get("id"):
                job.job_id = data.get("id")
            job.latitude = data.get("latitude")
            job.longitude = data.get("longitude")
            job.address = data.get("address")
            job.address_information = data.get("address_information")
            job.description = data.get("description")
            job.priority = True if data.get("priority") == "on" else False
            job.further_inspection = (
                True if data.get("further_inspection") == "on" else False
            )
            job.further_billing = True if data.get("further_billing") == "on" else False
            job.updated_by = user
            try:
                job.save()
            except IntegrityError as e:
                return JsonResponse(
                    {"IntegrityError": {"status": "כבר קיימת משימה עם מזהה זה"}}
                )

            if main_group_id:
                if int(main_group_id) in job_all_groups:
                    open_main_group_job.is_parent_group = False
                    open_main_group_job.save()
                    TransferJob.objects.filter(
                        job_id=transfer_job.job_id, group_id=main_group_id
                    ).update(is_parent_group=True)
                else:
                    open_main_group_job.group_id = int(main_group_id)
                    open_main_group_job.save()

            if delete_docs_id or delete_image_id:
                delete_attachment(delete_docs_id, delete_image_id)
            return JsonResponse({"job_update_status": "success"})

        if status in [
            JobStatus.CLOSE.label,
            JobStatus.CLOSE.value,
            JobStatus.PARTIAL.label,
            JobStatus.PARTIAL.value,
        ]:
            # Create close job bills lists
            for bill_string_dict in form_data:
                # convert string dict into dict
                bill_dict = json.loads(bill_string_dict)

                if bill_dict["measurement"] == "0" and bill_dict.get("close_bill_id"):
                    close_Job_bill = CloseJobBill.objects.filter(
                        id=bill_dict["close_bill_id"]
                    )
                    if close_Job_bill:
                        close_Job_bill.delete()

                # CloseJobBill object append into bulk_create_list
                if bill_dict["measurement"] != "0" and "close_bill_id" not in bill_dict:
                    bulk_create_list.append(
                        CloseJobBill(
                            name=bill_dict["bill_name"].strip(),
                            type_counting=bill_dict["type_counting"],
                            jumping_ration=(
                                decimal.Decimal(bill_dict["jumping_ration"])
                                if bill_dict["jumping_ration"] != "null"
                                else None
                            ),
                            type=bill_dict["type"],
                            job_id=bill_dict["job_id"],
                            created_by=user,
                            updated_by=user,
                            image=bill_dict["image"] if "image" in bill_dict else None,
                            measurement=decimal.Decimal(bill_dict["measurement"]),
                            is_created=True,
                        )
                    )

                # CloseJobBill object append into bulk_update_list
                if bill_dict["measurement"] != "0" and "close_bill_id" in bill_dict:
                    bulk_update_list.append(
                        CloseJobBill(
                            id=bill_dict["close_bill_id"],
                            name=bill_dict["bill_name"].strip(),
                            type=bill_dict["type"],
                            job_id=bill_dict["job_id"],
                            updated_by=user,
                            measurement=decimal.Decimal(bill_dict["measurement"]),
                        )
                    )

            if job.job_id != data.get("id"):
                job.job_id = data.get("id")
            job.latitude = data.get("latitude")
            job.longitude = data.get("longitude")
            job.address = data.get("address")
            job.address_information = data.get("address_information")
            job.description = data.get("description")

            if update_status == "true":
                transfer_job.further_billing = (
                    True if data.get("further_billing") == "on" else False
                )
                transfer_job.further_inspection = (
                    True if data.get("further_inspection") == "on" else False
                )
                transfer_job.updated_by = user
                transfer_job.save()

                job.priority = True if data.get("priority") == "on" else False
                job.further_inspection = (
                    True if data.get("further_inspection") == "on" else False
                )
                job.further_billing = (
                    True if data.get("further_billing") == "on" else False
                )
                job.updated_by = user

            job.closed_by = user

            if (
                transfer_job.job.status == JobStatus.CLOSE.value
                and status == JobStatus.CLOSE.value
            ):
                job_log = JobLog.objects.create(
                    job=job, updated_by=user, status="Update", created_at=timezone.now()
                )
            elif status == JobStatus.CLOSE.value:
                job_log = JobLog.objects.create(
                    job=transfer_job.job,
                    closed_by=user,
                    status="Close",
                    created_at=timezone.now(),
                )
            elif status == JobStatus.PARTIAL.value:
                job_log = JobLog.objects.create(
                    job=transfer_job.job,
                    partially_closed_by=user,
                    status="Partial",
                    created_at=timezone.now(),
                )
            try:
                job.save()
            except IntegrityError as e:
                return JsonResponse(
                    {"IntegrityError": {"status": "כבר קיימת משימה עם מזהה זה"}}
                )

            if main_group_id:
                if int(main_group_id) in job_all_groups:
                    update_main_group = TransferJob.objects.get(
                        job_id=transfer_job.job_id, is_parent_group=True
                    )
                    update_main_group.is_parent_group = False
                    update_main_group.save()
                    TransferJob.objects.filter(
                        job_id=transfer_job.job_id, group_id=main_group_id
                    ).update(is_parent_group=True)
                else:
                    update_parent_group = TransferJob.objects.get(
                        job_id=transfer_job.job_id, is_parent_group=True
                    )
                    update_parent_group.group_id = int(main_group_id)
                    update_parent_group.save()

            # Create Close job
            if status in [JobStatus.CLOSE.label, JobStatus.CLOSE.value]:
                # close job from all transferred group and false is_active
                close_main_group_job = TransferJob.objects.get(
                    job_id=transfer_job.job_id, is_parent_group=True
                )
                all_transferred_jobs = TransferJob.objects.filter(
                    job_id=transfer_job.job_id, is_parent_group=False
                )
                all_transferred_jobs.update(
                    status=JobStatus.CLOSE.value, is_active=False
                )

                # close job from main group and true is_active
                close_main_group_job.status = JobStatus.CLOSE.value
                close_main_group_job.job.status = JobStatus.CLOSE.value
                if close_main_group_job.job.closed_at == None:
                    close_main_group_job.job.closed_at = timezone.now()
                close_main_group_job.further_billing = further_billing
                close_main_group_job.job.further_billing = further_billing
                close_main_group_job.is_active = True
                close_main_group_job.save()
                close_main_group_job.job.save()
                CloseJobBill.objects.bulk_update(bulk_update_list, ["measurement"])
                CloseJobBill.objects.bulk_create(bulk_create_list)

                # Send notification on Close Job
                if update_status != "true":
                    members = (close_main_group_job.group.member.all()).exclude(
                        role__title=UserRoleChoices.GROUP_MANAGER.value
                    )
                    CreateNotification(
                        self,
                        message=_("Job is Closed by @"),
                        notification_type="Close",
                        job_id=transfer_job_id,
                        members=members,
                    )
                if delete_docs_id or delete_image_id:
                    delete_attachment(delete_docs_id, delete_image_id)
                return JsonResponse(
                    {
                        "job_close_or_update_status": "success",
                        "main_group_job": close_main_group_job.id,
                    }
                )

            # Create Partial job
            elif status in [JobStatus.PARTIAL.label, JobStatus.PARTIAL.value]:
                # partial close job from transferred group
                transfer_job.status = JobStatus.PARTIAL.value
                transfer_job.job.status = JobStatus.PARTIAL.value
                transfer_job.further_billing = further_billing
                transfer_job.job.further_billing = further_billing
                transfer_job.save()
                transfer_job.job.save()
                CloseJobBill.objects.bulk_update(bulk_update_list, ["measurement"])
                CloseJobBill.objects.bulk_create(bulk_create_list)
                if delete_docs_id or delete_image_id:
                    delete_attachment(delete_docs_id, delete_image_id)
                return JsonResponse({"job_partial_close_or_update_status": "success"})

        # {"error": {"status": The status is invalid}}
        return JsonResponse({"error": {"status": "הסטטוס אינו חוקי"}})


# Create for Notification Module
def CreateNotification(self, message, notification_type, job_id, members):
    current_user = self.request.user
    sender_name = (
        current_user.user_name if current_user.user_name else current_user.email
    )
    create_list = []
    for member in members:
        if member.id != current_user.id:
            create_list.append(
                Notification(
                    message=f"{message}{sender_name}",
                    notification_type=notification_type,
                    created_by_id=current_user.id,
                    updated_by_id=current_user.id,
                    job_id=job_id,
                    receiver_id=member.id,
                    sender_id=current_user.id,
                )
            )
    Notification.objects.bulk_create(create_list)


def JobApprovedView(request):
    job_id = request.GET.get("job_id")
    TransferJob.objects.filter(id=job_id).update(
        is_reviewed=request.GET.get("is_reviewed")
    )
    return JsonResponse({"Approved": {"status": "Job is Approved"}})


class DeleteOpenCloseJob(DeleteView):
    model = Job
    success_url = reverse_lazy("index")
    queryset = Job.objects.all()


def delete_attachment(delete_docs_id, delete_image_id):
    if delete_docs_id:
        docs_id_list = [int(id) for id in delete_docs_id.split(",")]
        JobAttachment.objects.filter(id__in=docs_id_list).delete()

    if delete_image_id:
        image_id_list = [int(id) for id in delete_image_id.split(",")]
        JobImage.objects.filter(id__in=image_id_list).delete()


class JobListDetails(ListView):
    model = Job
    queryset = Job.objects.all()
    template_name = "job_list_details.html"
    URL = os.environ["URL"]

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        job_ids = self.request.GET.get("jobIds")
        if job_ids:
            job_ids_list = job_ids.split(",")
            jobs = TransferJob.objects.filter(id__in=job_ids_list)
            context["context"] = jobs
            return context


@method_decorator(login_required, name="dispatch")
class generatejoblistpdf(View):
    model = Job
    template_name = "web_open_job_report.html"
    queryset = Job.objects.all()
    success_url = reverse_lazy("jobs:job-lists-details")
    URL = os.environ["URL"]

    def get(self, request, *args, **kwargs):
        job_ids = request.GET.get("jobIds")
        report_with_image = (
            request.GET.get("report_with_image", "false").lower() == "true"
        )
        if job_ids:
            job_ids_list = job_ids.split(",")
            jobs = TransferJob.objects.filter(id__in=job_ids_list)
            jobs_dict = {str(job.id): job for job in jobs}

            data = []
            for job_id in job_ids_list:
                instance = jobs_dict.get(job_id)

                new_dict = {}

                new_dict["id"] = instance.job.id
                new_dict["job_id"] = instance.job.job_id
                new_dict["description"] = instance.job.description
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
                    new_dict["images"] = instance.job.job_image.exclude(
                        image__regex="|".join(extensions)
                    )
                data.append(new_dict)
            data = {
                "context": data,
                "url": self.URL,
            }
            file_name = "job-list-Detail-report"
            pdf = generate_pdf(
                "web_open_job_report.html", self.request, data, file_name
            )
            path = os.path.join(settings.BASE_DIR, "media")
            response = FileResponse(
                open(f"{path}/{file_name}.pdf", "rb"),
                content_type="application/pdf",
            )
            return response


@method_decorator(login_required, name="dispatch")
class JobList(ListView):
    model = TransferJob
    queryset = TransferJob.objects.exclude(group__is_archive=False, is_active=False)

    def get(self, request, *args, **kwargs):
        jobs_status = request.GET.get("job_status")
        from_date = request.GET.get("from_date")
        to_date = request.GET.get("to_date")
        group = request.GET.get("group")
        current_user = self.request.user
        if group:
            if not current_user.is_superuser:
                group_id = (
                    Group.objects.filter(member=current_user.id, name=group)
                    .exclude(is_archive=True)
                    .values_list("id", flat=True)
                    .first()
                )

            group_id = (
                Group.objects.filter(name=group)
                .exclude(is_archive=True)
                .values_list("id", flat=True)
                .first()
            )

            self.request.session["previous_group_id"] = group_id

        job_values = [
            "id",
            "job__id",
            "job__address",
            "group__name",
            "status",
            "job__priority",
        ]

        if from_date != "None" and to_date != "None":
            jobs = (
                self.get_queryset()
                .filter(
                    group__name=group,
                    created_at__date__range=[from_date, to_date],
                    status=jobs_status,
                )
                .values(*job_values)
                if current_user.is_superuser == True
                else self.get_queryset()
                .filter(
                    group__name=group,
                    created_at__date__range=[from_date, to_date],
                    group__member=current_user.id,
                    status=jobs_status,
                )
                .values(*job_values)
            )
        else:
            jobs = (
                self.get_queryset()
                .filter(group__name=group, status=jobs_status)
                .values(*job_values)
                if current_user.is_superuser == True
                else self.get_queryset()
                .filter(
                    group__name=group, group__member=current_user.id, status=jobs_status
                )
                .values(*job_values)
            )

        job_list = []
        for job in jobs:
            job_id = job["job__id"]
            job_images = JobImage.objects.filter(job__id=job_id)
            if job_images.exists():
                first_image = job_images.first().image.url
            else:
                first_image = None

            job_data = {
                "job_id": job_id,
                "job_address": job["job__address"],
                "status": job["status"],
                "group": job["group__name"],
                "job_priority": job["job__priority"],
                "first_image": first_image,
            }
            job_list.append(job_data)

        response = {"job_list": job_list}
        return JsonResponse(response, safe=False)


# Multiple TransferJob Module
@method_decorator(login_required, name="dispatch")
class MultipleTransferJobView(CreateView):
    model = TransferJob
    template_name = "job_detail.html"

    def post(self, request, *args, **kwargs):
        jobs_list = request.GET.get("multiple_jobs")
        data = request.POST.copy()
        transfer_group = data["group"]
        create_list = []

        jobs = jobs_list.split(",")
        jobs = TransferJob.objects.filter(id__in=jobs)

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

                TransferJob.objects.filter(group=transfer_group, job=job.job_id).update(
                    status=JobStatus.OPEN.value, is_active=True
                )
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
            transferred_by=self.request.user,
            status="Transfer",
            created_at=timezone.now(),
        )
        TransferJob.objects.bulk_create(create_list)
        return JsonResponse({"job_transfer_status": "success"})


class ReturnJobNotes(UpdateView):
    model = ReturnJob
    form_class = ReturnJobNotesForm

    def get_success_url(self):
        next_url = self.request.POST.get("next")
        if next_url:
            return next_url
        return reverse_lazy("jobs:return-job-list")


def get_return_job_notes(request, pk):
    return_job = ReturnJob.objects.get(id=pk)
    return JsonResponse({"notes": return_job.notes})
