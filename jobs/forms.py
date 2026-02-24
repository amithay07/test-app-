from django import forms

from users.models.job import Job
from users.models.job import RecentSearchJob
from users.models.job import ReturnJob
from users.models.job import TransferJob


class CreateJobForm(forms.ModelForm):
    class Meta:
        model = Job
        fields = "__all__"


class ReturnJobForm(forms.ModelForm):
    class Meta:
        model = ReturnJob
        fields = ["job", "status", "comment", "duplicate", "group"]


class ReturnJobNotesForm(forms.ModelForm):
    class Meta:
        model = ReturnJob
        fields = ["notes"]


class RecentSearchJobForm(forms.ModelForm):
    class Meta:
        model = RecentSearchJob
        fields = "__all__"


class TransferJobForm(forms.ModelForm):
    class Meta:
        model = TransferJob
        fields = "__all__"


class CloseJobForm(forms.ModelForm):
    class Meta:
        model = Job
        fields = "__all__"
