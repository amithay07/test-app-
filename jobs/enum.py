from django.db import models
from django.utils.translation import gettext_lazy as _


class SortBy(models.TextChoices):
    ASCENDING = "ASCENDING", _("ASCENDING")
    DESCENDING = "DESCENDING", _("DESCENDING")
