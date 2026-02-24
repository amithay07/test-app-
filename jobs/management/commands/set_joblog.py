from django.core.management.base import BaseCommand
from users.models import Job, JobLog

class Command(BaseCommand):
    help = 'Create JobLog entries for jobs with specified conditions'

    def handle(self, *args, **options):
        # Fetch all jobs
        all_jobs = Job.objects.all()

        for job in all_jobs:
            # Check if the job has both created_at and created_by fields
            if job.created_at and job.created_by:
                JobLog.objects.create(job=job, created_by=job.created_by, created_at=job.created_at, status='Create')
            
            # Check if the job has both updated_at and updated_by fields
            if job.updated_at and job.updated_by:
                # if job.status == 
                JobLog.objects.create(job=job, updated_by=job.updated_by, created_at=job.updated_at, status='Update')
            
            # Check if the job has both closed_at and closed_by fields
            if job.closed_at and job.closed_by:
                JobLog.objects.create(job=job, closed_by=job.closed_by, created_at=job.closed_at, status='Close')

        self.stdout.write(self.style.SUCCESS('Job logs creation completed successfully'))
