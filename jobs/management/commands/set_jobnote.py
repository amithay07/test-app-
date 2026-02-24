from django.core.management.base import BaseCommand

from users.models import Job
from users.models import JobNote
from users.models import JobStatus


class Command(BaseCommand):
    help = 'Migrate job notes from Job model to JobNote model for closed jobs with notes'

    def handle(self, *args, **options):
        # Fetch all closed jobs with notes
        closed_jobs_with_notes = Job.objects.filter(status=JobStatus.CLOSE.value, notes__isnull=False)

        for job in closed_jobs_with_notes:
            # Create JobNote entry for each note in the notes field of the Job model
            if job.notes:
                JobNote.objects.create(job=job, note=job.notes, created_by=job.closed_by, updated_by=job.closed_by)

        self.stdout.write(self.style.SUCCESS('Job notes migration completed successfully'))
