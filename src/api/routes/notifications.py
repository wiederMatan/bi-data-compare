"""Notification API routes."""

from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.core.logging import get_logger
from src.services.notifications import get_notification_service
from src.services.persistence import get_persistence_service

logger = get_logger(__name__)
router = APIRouter()


class EmailConfigRequest(BaseModel):
    """Request to configure email settings."""

    smtp_server: str = Field(..., description="SMTP server address")
    smtp_port: int = Field(587, description="SMTP port")
    username: str = Field(..., description="SMTP username")
    password: str = Field(..., description="SMTP password")
    sender_email: Optional[str] = Field(None, description="Sender email (defaults to username)")
    use_tls: bool = Field(True, description="Use TLS")


class SendEmailRequest(BaseModel):
    """Request to send email."""

    to: list[str] = Field(..., description="Recipient email addresses")
    subject: str = Field(..., description="Email subject")
    body: str = Field(..., description="Email body (plain text)")
    html_body: Optional[str] = Field(None, description="HTML body")


class SendReportRequest(BaseModel):
    """Request to send comparison report."""

    to: list[str] = Field(..., description="Recipient email addresses")
    run_id: str = Field(..., description="Comparison run ID")
    include_details: bool = Field(True, description="Include detailed results")


class SendAlertRequest(BaseModel):
    """Request to send alert."""

    to: list[str] = Field(..., description="Recipient email addresses")
    alert_type: str = Field(..., description="Alert type: error, warning, info")
    message: str = Field(..., description="Alert message")
    details: Optional[dict] = Field(None, description="Additional details")


@router.post("/configure")
async def configure_email(request: EmailConfigRequest):
    """
    Configure email notification settings.

    Sets up SMTP server connection for sending notifications.
    """
    try:
        service = get_notification_service()
        service.configure(
            smtp_server=request.smtp_server,
            smtp_port=request.smtp_port,
            username=request.username,
            password=request.password,
            sender_email=request.sender_email,
            use_tls=request.use_tls,
        )

        return {
            "message": "Email configured successfully",
            "smtp_server": request.smtp_server,
            "smtp_port": request.smtp_port,
        }

    except Exception as e:
        logger.error(f"Failed to configure email: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status")
async def get_notification_status():
    """
    Get notification service status.

    Returns whether email notifications are configured and enabled.
    """
    service = get_notification_service()
    return {
        "enabled": service.is_enabled(),
        "configured": service.config is not None,
    }


@router.post("/send")
async def send_email(request: SendEmailRequest):
    """
    Send a custom email.

    Sends email to specified recipients with custom content.
    """
    try:
        service = get_notification_service()

        if not service.is_enabled():
            raise HTTPException(
                status_code=400,
                detail="Email notifications not configured. Call /configure first.",
            )

        success = service.send_email(
            to_addresses=request.to,
            subject=request.subject,
            body_text=request.body,
            body_html=request.html_body,
        )

        if success:
            return {"message": "Email sent successfully", "recipients": request.to}
        else:
            raise HTTPException(status_code=500, detail="Failed to send email")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to send email: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/send-report")
async def send_report(request: SendReportRequest):
    """
    Send comparison report email.

    Sends formatted report for a completed comparison run.
    """
    try:
        service = get_notification_service()

        if not service.is_enabled():
            raise HTTPException(
                status_code=400,
                detail="Email notifications not configured. Call /configure first.",
            )

        # Get run data from persistence
        persistence = get_persistence_service()
        run = persistence.get_run(request.run_id)

        if not run:
            raise HTTPException(
                status_code=404,
                detail=f"Run {request.run_id} not found",
            )

        # Build info dictionaries
        source_info = {
            "server": run.get("source_server"),
            "database": run.get("source_database"),
        }
        target_info = {
            "server": run.get("target_server"),
            "database": run.get("target_database"),
        }
        results_summary = {
            "total_tables": run.get("total_tables", 0),
            "matching_tables": run.get("matching_tables", 0),
            "different_tables": run.get("different_tables", 0),
            "failed_tables": run.get("failed_tables", 0),
        }

        success = service.send_comparison_report(
            to_addresses=request.to,
            run_id=request.run_id,
            source_info=source_info,
            target_info=target_info,
            results_summary=results_summary,
            include_details=request.include_details,
        )

        if success:
            return {
                "message": "Report sent successfully",
                "recipients": request.to,
                "run_id": request.run_id,
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to send report")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to send report: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/send-alert")
async def send_alert(request: SendAlertRequest):
    """
    Send an alert notification.

    Sends alert email for errors, warnings, or information.
    """
    try:
        service = get_notification_service()

        if not service.is_enabled():
            raise HTTPException(
                status_code=400,
                detail="Email notifications not configured. Call /configure first.",
            )

        if request.alert_type not in ["error", "warning", "info"]:
            raise HTTPException(
                status_code=400,
                detail="alert_type must be 'error', 'warning', or 'info'",
            )

        success = service.send_alert(
            to_addresses=request.to,
            alert_type=request.alert_type,
            message=request.message,
            details=request.details,
        )

        if success:
            return {
                "message": "Alert sent successfully",
                "recipients": request.to,
                "alert_type": request.alert_type,
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to send alert")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to send alert: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/test")
async def test_email(to: str):
    """
    Send a test email.

    Verifies email configuration by sending a test message.
    """
    try:
        service = get_notification_service()

        if not service.is_enabled():
            raise HTTPException(
                status_code=400,
                detail="Email notifications not configured. Call /configure first.",
            )

        success = service.send_email(
            to_addresses=[to],
            subject="[BI Data Compare] Test Email",
            body_text="This is a test email from BI Data Compare.\n\nIf you received this, email notifications are working correctly.",
            body_html="""
            <html>
            <body style="font-family: Arial, sans-serif;">
                <h2>Test Email</h2>
                <p>This is a test email from BI Data Compare.</p>
                <p style="color: green;">If you received this, email notifications are working correctly.</p>
            </body>
            </html>
            """,
        )

        if success:
            return {"message": f"Test email sent to {to}"}
        else:
            raise HTTPException(status_code=500, detail="Failed to send test email")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to send test email: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
