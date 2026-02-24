from django.urls import path

from jobs.views import AddNewJobListView
from jobs.views import CloseJobBillView
from jobs.views import CloseJobFormView
from jobs.views import CreateCloseJobBill
from jobs.views import DeleteOpenCloseJob
from jobs.views import EditJobDetail
from jobs.views import GeneratePdf
from jobs.views import JobApprovedView
from jobs.views import JobCloseView
from jobs.views import JobCreateView
from jobs.views import JobDetailView
from jobs.views import JobList
from jobs.views import JobListView
from jobs.views import MultipleTransferJobView
from jobs.views import RecentSearchJob
from jobs.views import ReportGeneratorListView
from jobs.views import ReturnJobCreateView
from jobs.views import ReturnJobDeleteView
from jobs.views import ReturnJobDetailView
from jobs.views import ReturnJobListView
from jobs.views import ReturnJobNotes
from jobs.views import ReturnJobUpdateView
from jobs.views import TransferJobView
from jobs.views import JobListDetails
from jobs.views import generatejoblistpdf
from jobs.views import get_jobs_for_group
from jobs.views import get_return_job_notes

app_name = "jobs"
urlpatterns = [
    path("", JobListView.as_view(), name="job-list"),
    path("add-new-job/", AddNewJobListView.as_view(), name="add-new-job"),
    path("job-add/", JobCreateView.as_view(), name="job-create"),
    path("job-detail/<int:pk>/", JobDetailView.as_view(), name="jobdetail"),
    path("transfer-job/<int:pk>/", TransferJobView.as_view(), name="transfer-job"),
    path(
        "multiple-transfer-job/",
        MultipleTransferJobView.as_view(),
        name="multiple-transfer-job",
    ),
    path(
        "return-job-create/<int:pk>/",
        ReturnJobCreateView.as_view(),
        name="return-job-create",
    ),
    path("return_job_list/", ReturnJobListView.as_view(), name="return-job-list"),
    path(
        "return-job-detail/",
        ReturnJobDetailView.as_view(),
        name="return-job-details",
    ),
    path(
        "return_job_update/",
        ReturnJobUpdateView.as_view(),
        name="return-job-update",
    ),
    path("recent_search_job/", RecentSearchJob.as_view(), name="recent-search-job"),
    path("report_view/", ReportGeneratorListView.as_view(), name="report-generate"),
    path(
        "close_job_sign_bills/", CloseJobBillView.as_view(), name="close-job-sign-bills"
    ),
    path("close_job_forms/", CloseJobFormView.as_view(), name="close-job-forms"),
    path(
        "create_close_job_bills/",
        CreateCloseJobBill.as_view(),
        name="create-close-job-bills",
    ),
    path("close_job/", JobCloseView.as_view(), name="close-job"),
    path("generatePdf/", GeneratePdf.as_view(), name="generatePdf"),
    path(
        "get_jobs_for_group/<int:group_id>",
        get_jobs_for_group,
        name="get-jobs-for-group",
    ),
    path(
        "return_job_delete/<int:pk>/<str:return_job_type>/",
        ReturnJobDeleteView.as_view(),
        name="return-job-delete",
    ),
    path("job_detail/<int:pk>/", EditJobDetail.as_view(), name="edit-job-detail"),
    path("job_approved/", JobApprovedView, name="job-approved"),
    path("delete_job/<int:pk>/", DeleteOpenCloseJob.as_view(), name="delete-job"),
    path("jobs_list/", JobList.as_view(), name="jobs-list"),
    path(
        "return_job_notes/<int:pk>/", ReturnJobNotes.as_view(), name="return-job-notes"
    ),
    path(
        "get_return_job_notes/<int:pk>/",
        get_return_job_notes,
        name="get-return-job-notes",
    ),
    path("job_lists_details/",JobListDetails.as_view(),name="job-lists-details"),
    path('generate_job_list_pdf/',generatejoblistpdf.as_view(),name="job-list-pdf")
]
