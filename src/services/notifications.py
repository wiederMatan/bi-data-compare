"""Email notification service."""

import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Optional

from src.core.config import get_settings
from src.core.logging import get_logger

logger = get_logger(__name__)


class EmailConfig:
    """Email configuration."""

    def __init__(
        self,
        smtp_server: str = "smtp.gmail.com",
        smtp_port: int = 587,
        username: Optional[str] = None,
        password: Optional[str] = None,
        sender_email: Optional[str] = None,
        use_tls: bool = True,
    ):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.sender_email = sender_email or username
        self.use_tls = use_tls


class NotificationService:
    """Service for sending email notifications."""

    def __init__(self, config: Optional[EmailConfig] = None):
        """
        Initialize notification service.

        Args:
            config: Email configuration. If None, will try to load from settings.
        """
        self.config = config
        self._enabled = config is not None and config.username is not None

    def is_enabled(self) -> bool:
        """Check if email notifications are enabled."""
        return self._enabled

    def configure(
        self,
        smtp_server: str,
        smtp_port: int,
        username: str,
        password: str,
        sender_email: Optional[str] = None,
        use_tls: bool = True,
    ) -> None:
        """
        Configure email settings.

        Args:
            smtp_server: SMTP server address
            smtp_port: SMTP port
            username: SMTP username
            password: SMTP password
            sender_email: Sender email address (defaults to username)
            use_tls: Use TLS encryption
        """
        self.config = EmailConfig(
            smtp_server=smtp_server,
            smtp_port=smtp_port,
            username=username,
            password=password,
            sender_email=sender_email,
            use_tls=use_tls,
        )
        self._enabled = True
        logger.info(f"Email notifications configured: {smtp_server}:{smtp_port}")

    def send_email(
        self,
        to_addresses: list[str],
        subject: str,
        body_text: str,
        body_html: Optional[str] = None,
    ) -> bool:
        """
        Send an email.

        Args:
            to_addresses: List of recipient email addresses
            subject: Email subject
            body_text: Plain text body
            body_html: HTML body (optional)

        Returns:
            True if sent successfully
        """
        if not self._enabled or not self.config:
            logger.warning("Email notifications not configured")
            return False

        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = self.config.sender_email
            msg["To"] = ", ".join(to_addresses)

            # Attach text and HTML parts
            msg.attach(MIMEText(body_text, "plain"))
            if body_html:
                msg.attach(MIMEText(body_html, "html"))

            # Connect and send
            context = ssl.create_default_context()

            if self.config.use_tls:
                with smtplib.SMTP(self.config.smtp_server, self.config.smtp_port) as server:
                    server.starttls(context=context)
                    server.login(self.config.username, self.config.password)
                    server.sendmail(
                        self.config.sender_email,
                        to_addresses,
                        msg.as_string(),
                    )
            else:
                with smtplib.SMTP_SSL(
                    self.config.smtp_server,
                    self.config.smtp_port,
                    context=context,
                ) as server:
                    server.login(self.config.username, self.config.password)
                    server.sendmail(
                        self.config.sender_email,
                        to_addresses,
                        msg.as_string(),
                    )

            logger.info(f"Email sent to {to_addresses}: {subject}")
            return True

        except Exception as e:
            logger.error(f"Failed to send email: {str(e)}")
            return False

    def send_comparison_report(
        self,
        to_addresses: list[str],
        run_id: str,
        source_info: dict,
        target_info: dict,
        results_summary: dict,
        include_details: bool = True,
    ) -> bool:
        """
        Send comparison report email.

        Args:
            to_addresses: Recipient email addresses
            run_id: Comparison run ID
            source_info: Source database info
            target_info: Target database info
            results_summary: Summary of results
            include_details: Include detailed results

        Returns:
            True if sent successfully
        """
        subject = self._build_report_subject(results_summary)
        body_text = self._build_report_text(
            run_id, source_info, target_info, results_summary
        )
        body_html = self._build_report_html(
            run_id, source_info, target_info, results_summary, include_details
        )

        return self.send_email(to_addresses, subject, body_text, body_html)

    def send_alert(
        self,
        to_addresses: list[str],
        alert_type: str,
        message: str,
        details: Optional[dict] = None,
    ) -> bool:
        """
        Send an alert notification.

        Args:
            to_addresses: Recipient email addresses
            alert_type: Type of alert (error, warning, info)
            message: Alert message
            details: Additional details

        Returns:
            True if sent successfully
        """
        subject = f"[BI Data Compare] {alert_type.upper()}: {message[:50]}"

        body_text = f"""
BI Data Compare Alert

Type: {alert_type.upper()}
Message: {message}
"""
        if details:
            body_text += "\nDetails:\n"
            for key, value in details.items():
                body_text += f"  {key}: {value}\n"

        body_html = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; }}
        .alert {{ padding: 15px; border-radius: 5px; margin: 10px 0; }}
        .alert-error {{ background: #f8d7da; border: 1px solid #f5c6cb; color: #721c24; }}
        .alert-warning {{ background: #fff3cd; border: 1px solid #ffeeba; color: #856404; }}
        .alert-info {{ background: #d1ecf1; border: 1px solid #bee5eb; color: #0c5460; }}
    </style>
</head>
<body>
    <h2>BI Data Compare Alert</h2>
    <div class="alert alert-{alert_type}">
        <strong>{alert_type.upper()}</strong>: {message}
    </div>
"""
        if details:
            body_html += "<h3>Details</h3><ul>"
            for key, value in details.items():
                body_html += f"<li><strong>{key}:</strong> {value}</li>"
            body_html += "</ul>"

        body_html += "</body></html>"

        return self.send_email(to_addresses, subject, body_text, body_html)

    def _build_report_subject(self, results_summary: dict) -> str:
        """Build email subject based on results."""
        total = results_summary.get("total_tables", 0)
        matching = results_summary.get("matching_tables", 0)
        different = results_summary.get("different_tables", 0)
        failed = results_summary.get("failed_tables", 0)

        if failed > 0:
            return f"[BI Data Compare] Comparison Failed - {failed} errors"
        elif different > 0:
            return f"[BI Data Compare] Differences Found - {different}/{total} tables"
        else:
            return f"[BI Data Compare] All {total} Tables Match"

    def _build_report_text(
        self,
        run_id: str,
        source_info: dict,
        target_info: dict,
        results_summary: dict,
    ) -> str:
        """Build plain text report."""
        return f"""
BI Data Compare - Comparison Report

Run ID: {run_id}

Source Database:
  Server: {source_info.get('server')}
  Database: {source_info.get('database')}

Target Database:
  Server: {target_info.get('server')}
  Database: {target_info.get('database')}

Results Summary:
  Total Tables: {results_summary.get('total_tables', 0)}
  Matching: {results_summary.get('matching_tables', 0)}
  Different: {results_summary.get('different_tables', 0)}
  Failed: {results_summary.get('failed_tables', 0)}

---
This is an automated notification from BI Data Compare.
"""

    def _build_report_html(
        self,
        run_id: str,
        source_info: dict,
        target_info: dict,
        results_summary: dict,
        include_details: bool = True,
    ) -> str:
        """Build HTML report."""
        total = results_summary.get("total_tables", 0)
        matching = results_summary.get("matching_tables", 0)
        different = results_summary.get("different_tables", 0)
        failed = results_summary.get("failed_tables", 0)

        # Determine status color
        if failed > 0:
            status_color = "#DC3545"
            status_text = "Failed"
        elif different > 0:
            status_color = "#FFC107"
            status_text = "Differences Found"
        else:
            status_color = "#28A745"
            status_text = "All Match"

        html = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }}
        .container {{ max-width: 600px; margin: 0 auto; background: white; padding: 20px; border-radius: 5px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        h1 {{ color: #333; border-bottom: 2px solid #0066CC; padding-bottom: 10px; }}
        .status {{ padding: 10px; border-radius: 5px; margin: 15px 0; color: white; font-weight: bold; }}
        .info-box {{ background: #f8f9fa; padding: 15px; border-radius: 5px; margin: 10px 0; }}
        .metrics {{ display: flex; justify-content: space-between; margin: 20px 0; }}
        .metric {{ text-align: center; padding: 15px; background: #f8f9fa; border-radius: 5px; flex: 1; margin: 0 5px; }}
        .metric .value {{ font-size: 24px; font-weight: bold; }}
        .metric .label {{ color: #666; font-size: 12px; }}
        .match {{ color: #28A745; }}
        .diff {{ color: #FFC107; }}
        .fail {{ color: #DC3545; }}
        .footer {{ margin-top: 20px; padding-top: 15px; border-top: 1px solid #ddd; color: #666; font-size: 12px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Database Comparison Report</h1>

        <div class="status" style="background: {status_color};">
            {status_text}
        </div>

        <p><strong>Run ID:</strong> {run_id}</p>

        <div class="info-box">
            <h3 style="margin-top: 0;">Source Database</h3>
            <p>Server: {source_info.get('server')}<br>
            Database: {source_info.get('database')}</p>
        </div>

        <div class="info-box">
            <h3 style="margin-top: 0;">Target Database</h3>
            <p>Server: {target_info.get('server')}<br>
            Database: {target_info.get('database')}</p>
        </div>

        <h3>Results Summary</h3>
        <div class="metrics">
            <div class="metric">
                <div class="value">{total}</div>
                <div class="label">Total Tables</div>
            </div>
            <div class="metric">
                <div class="value match">{matching}</div>
                <div class="label">Matching</div>
            </div>
            <div class="metric">
                <div class="value diff">{different}</div>
                <div class="label">Different</div>
            </div>
            <div class="metric">
                <div class="value fail">{failed}</div>
                <div class="label">Failed</div>
            </div>
        </div>

        <div class="footer">
            This is an automated notification from BI Data Compare.
        </div>
    </div>
</body>
</html>
"""
        return html


# Global singleton
_notification_service: Optional[NotificationService] = None


def get_notification_service() -> NotificationService:
    """Get global notification service instance."""
    global _notification_service
    if _notification_service is None:
        _notification_service = NotificationService()
    return _notification_service
