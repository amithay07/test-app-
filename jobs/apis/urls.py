from django.urls import path

from jobs.apis.views import AddDuplicateJobReference
from jobs.apis.views import CloseJobBillView
from jobs.apis.views import DeleteJobView
from jobs.apis.views import GroupJobView
from jobs.apis.views import JobCreateView
from jobs.apis.views import JobIsReviewed
from jobs.apis.views import JobNotification
from jobs.apis.views import JobTransferView
from jobs.apis.views import MapJobView
from jobs.apis.views import MultipleJobTransferView
from jobs.apis.views import OpenJobPdfGeneratorView
from jobs.apis.views import PdfGeneratorView
from jobs.apis.views import RecentAddJobView
from jobs.apis.views import RecentReturnJobView
from jobs.apis.views import RecentSearchJobsListCreateView
from jobs.apis.views import RecentTransferJob
from jobs.apis.views import ReportGeneratorView
from jobs.apis.views import ReturnJobUpdateView
from jobs.apis.views import ReturnJobView
from jobs.apis.views import MultiplePdfGeneratorView

urlpatterns = [
    path("", JobCreateView.as_view({"get": "list", "post": "create"}), name="jobs"),
    path(
        "<int:pk>/",
        JobCreateView.as_view({"get": "retrieve", "patch": "partial_update"}),
        name="job-detail",
    ),
    path("transfer-job/", JobTransferView.as_view(), name="transfer-job"),
    path(
        "recent-transfer-job/", RecentTransferJob.as_view(), name="recent-transfer-job"
    ),
    path("group-jobs/", GroupJobView.as_view(), name="group-jobs"),
    path("map-jobs/", MapJobView.as_view(), name="map-jobs"),
    path(
        "return-job/",
        ReturnJobView.as_view({"get": "list", "post": "create"}),
        name="return-job",
    ),
    path(
        "return-job/<int:pk>/",
        ReturnJobUpdateView.as_view(
            {"get": "retrieve", "patch": "partial_update", "delete": "destroy"}
        ),
        name="return-job-detail",
    ),
    path("recent-return-job/", RecentReturnJobView.as_view(), name="recent-return-job"),
    path("recent-add-job/", RecentAddJobView.as_view(), name="recent-add-jobs"),
    path("job-notification/", JobNotification.as_view(), name="job-notification"),
    path(
        "recent-search-job/",
        RecentSearchJobsListCreateView.as_view(),
        name="recent-search-job",
    ),
    path("report-view/", ReportGeneratorView.as_view(), name="report-generate"),
    path("create-pdf/", PdfGeneratorView.as_view(), name="create-pdf"),
    path("multiple-jobs-create-pdf/",MultiplePdfGeneratorView.as_view(),name="multiple-job-reprot-creation"),
    path("create-open-jobs-pdf/", OpenJobPdfGeneratorView.as_view(), name="create-open-jobs-pdf"),
    path(
        "close-job-bill/",
        CloseJobBillView.as_view({"get": "list", "post": "create"}),
        name="close-job-bill",
    ),
    path(
        "close-job-bill/<int:pk>/",
        CloseJobBillView.as_view({"patch": "partial_update", "get": "retrieve"}),
        name="close-job-bill",
    ),
    path(
        "duplicate-job-reference/",
        AddDuplicateJobReference.as_view(),
        name="duplicate-job-reference",
    ),
    path(
        "job-reviewed/",
        JobIsReviewed.as_view(),
        name="job_reviewed",
    ),
    path(
        "job-delete/<int:pk>/",
        DeleteJobView.as_view(),
        name="job_delete",
    ),
    path(
        "multiple-transfer-job/",
        MultipleJobTransferView.as_view(),
        name="multiple-transfer-job",
    ),
]
