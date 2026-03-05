"""PDF and text report generator for DQA validation results."""

import hashlib
import os
from html import escape
from typing import Dict, Any, Optional, List
from datetime import datetime

from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Flowable
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT

from clifpy.utils.rule_codes import enrich_issue, truncate_comment


class YearlySparkBar(Flowable):
    """Mini bar chart showing per-year unique ID counts for a category value.

    Bars are colored blue when the value is present, red when absent.
    """

    def __init__(self, yearly_counts: Dict[int, int], width: float = 2.5 * inch,
                 height: float = 20):
        super().__init__()
        self.yearly_counts = yearly_counts
        self.width = width
        self.height = height

    _LABEL_HEIGHT = 8  # space reserved for year tick labels

    def wrap(self, availWidth, availHeight):
        return self.width, self.height + self._LABEL_HEIGHT

    def draw(self):
        if not self.yearly_counts:
            return
        years = sorted(self.yearly_counts.keys())
        n = len(years)
        if n == 0:
            return
        max_count = max(self.yearly_counts.values()) or 1
        gap = 0.5
        bar_w = max(1, (self.width - gap * (n - 1)) / n)
        present_color = colors.HexColor('#4A90D9')
        absent_color = colors.HexColor('#E74C3C')
        label_offset = self._LABEL_HEIGHT  # bars drawn above label area
        for i, year in enumerate(years):
            count = self.yearly_counts[year]
            x = i * (bar_w + gap)
            if count > 0:
                bar_h = max(2, (count / max_count) * self.height)
                self.canv.setFillColor(present_color)
            else:
                bar_h = self.height  # full height red bar for absent
                self.canv.setFillColor(absent_color)
            self.canv.rect(x, label_offset, bar_w, bar_h, stroke=0, fill=1)
        # Draw year tick labels for first and last bars
        self.canv.setFillColor(colors.HexColor('#666666'))
        self.canv.setFont('Helvetica', 5.5)
        first_x = 0
        self.canv.drawString(first_x, 0, str(years[0]))
        if n > 1:
            last_x = (n - 1) * (bar_w + gap)
            self.canv.drawString(last_x, 0, str(years[-1]))

DQA_CATEGORIES = ('conformance', 'completeness', 'plausibility')


def _make_error_id(issue: Dict[str, Any]) -> str:
    """Create a unique identifier for a DQA issue, matching feedback.create_error_id."""
    msg = issue.get('message', '')
    desc_hash = hashlib.md5(msg.encode()).hexdigest()[:8]
    category = issue.get('category', '')
    check_type = issue.get('check_type', 'unknown')
    prefix = f"{category}_{check_type}" if category else check_type
    prefix = prefix.replace(' ', '_').lower()
    return f"{prefix}_{desc_hash}"


def collect_dqa_issues(validation_data: Dict[str, Any]):
    """Collect errors, warnings, and info messages from run_full_dqa output.

    Returns (category_scores, all_issues) where each issue is a dict with
    category, check_type, severity ('error'/'warning'/'info'), message, details,
    plus enriched fields: rule_code, rule_description, column_field.
    """
    category_scores = {}
    all_issues: List[Dict[str, Any]] = []

    for category in DQA_CATEGORIES:
        checks = validation_data.get(category, {})
        if not checks:
            continue
        for check_name, d in checks.items():
            for err in d['errors']:
                issue = {
                    'category': category,
                    'check_type': d['check_type'],
                    'severity': 'error',
                    'message': err.get('message', ''),
                    'details': err.get('details', {}),
                }
                enriched = enrich_issue(issue, check_key=check_name)
                if enriched is not None:
                    all_issues.append(enriched)
            for warn in d['warnings']:
                issue = {
                    'category': category,
                    'check_type': d['check_type'],
                    'severity': 'warning',
                    'message': warn.get('message', ''),
                    'details': warn.get('details', {}),
                }
                enriched = enrich_issue(issue, check_key=check_name)
                if enriched is not None:
                    all_issues.append(enriched)
            for info_msg in d.get('info', []):
                issue = {
                    'category': category,
                    'check_type': d['check_type'],
                    'severity': 'info',
                    'message': info_msg.get('message', ''),
                    'details': info_msg.get('details', {}),
                }
                enriched = enrich_issue(issue, check_key=check_name)
                if enriched is not None:
                    all_issues.append(enriched)

    # Compute scores from enriched findings so summary matches detail counts
    for category in DQA_CATEGORIES:
        cat_issues = [i for i in all_issues if i['category'] == category]
        if cat_issues:
            cat_passed = sum(1 for i in cat_issues if i['severity'] in ('info', 'warning'))
            category_scores[category] = (cat_passed, len(cat_issues))

    return category_scores, all_issues


def compute_table_stats(df, schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Compute per-column descriptive stats for schema-defined columns.

    Parameters
    ----------
    df : pd.DataFrame or None
        The table's DataFrame.
    schema : dict or None
        The table schema with a ``columns`` list.

    Returns
    -------
    list[dict]
        One dict per column with keys: column, dtype, null_count,
        null_pct, unique.  Empty list when inputs are missing/empty.
    """
    if df is None or schema is None:
        return []
    try:
        n_rows = len(df)
    except Exception:
        return []
    if n_rows == 0:
        return []

    schema_cols = [c['name'] for c in schema.get('columns', [])]
    stats: List[Dict[str, Any]] = []
    for col_def in schema.get('columns', []):
        col_name = col_def['name']
        if col_name not in df.columns:
            continue
        series = df[col_name]
        null_count = int(series.isna().sum())
        null_pct = round(null_count / n_rows * 100, 1) if n_rows else 0.0
        unique = int(series.nunique(dropna=True))
        stats.append({
            'column': col_name,
            'dtype': col_def.get('data_type', str(series.dtype)),
            'null_count': null_count,
            'null_pct': null_pct,
            'unique': unique,
        })
    return stats


def generate_validation_pdf(validation_data: Dict[str, Any],
                            table_name: str, output_path: str,
                            site_name: Optional[str] = None,
                            feedback: Optional[Dict[str, Any]] = None) -> str:
    """
    Generate a PDF report from DQA validation results.

    Parameters
    ----------
    validation_data : dict
        Output from run_full_dqa (keys: conformance, completeness,
        relational, plausibility).
    table_name : str
        Name of the table.
    output_path : str
        Path where PDF should be saved.
    site_name : str, optional
        Name of the site/hospital.
    feedback : dict, optional
        User feedback with 'user_decisions' keyed by error_id.

    Returns
    -------
    str
        Path to generated PDF file.
    """
    category_scores, all_issues = collect_dqa_issues(validation_data)

    # Build feedback lookup: error_id -> decision dict
    feedback_lookup: Dict[str, Dict[str, Any]] = {}
    if feedback and feedback.get('user_decisions'):
        feedback_lookup = feedback['user_decisions']

    # Build set of rejected error IDs so we can adjust summary counts
    rejected_ids: set = {
        eid for eid, d in feedback_lookup.items()
        if d.get('decision') == 'rejected'
    }

    total_passed = sum(p for p, _ in category_scores.values())
    total_checks = sum(t for _, t in category_scores.values())
    error_count = sum(1 for i in all_issues if i['severity'] == 'error')
    warning_count = sum(1 for i in all_issues if i['severity'] == 'warning')

    # Adjust counts: rejected errors no longer count as errors
    if rejected_ids:
        rejected_error_count = sum(
            1 for i in all_issues
            if i['severity'] == 'error' and _make_error_id(i) in rejected_ids
        )
        error_count -= rejected_error_count
        total_passed += rejected_error_count

    # --- Build PDF ---
    doc = SimpleDocTemplate(output_path, pagesize=letter)
    story = []
    styles = getSampleStyleSheet()

    primary_color = colors.HexColor('#1F4E79')
    text_dark = colors.HexColor('#2C3E50')
    text_medium = colors.HexColor('#5D6D7E')
    header_bg = colors.HexColor('#F5F6FA')
    pass_bg = colors.HexColor('#E8F5E8')
    fail_bg = colors.HexColor('#FFEAEA')
    warn_bg = colors.HexColor('#FFF3E0')

    title_style = ParagraphStyle(
        'CustomTitle', parent=styles['Heading1'],
        fontSize=22, textColor=primary_color, spaceAfter=24,
        alignment=TA_CENTER, fontName='Helvetica-Bold',
    )
    heading_style = ParagraphStyle(
        'CustomHeading', parent=styles['Heading2'],
        fontSize=14, textColor=text_dark, spaceAfter=10,
        spaceBefore=16, fontName='Helvetica-Bold',
    )
    timestamp_style = ParagraphStyle(
        'TimestampStyle', parent=styles['Normal'],
        fontSize=8, textColor=text_medium, alignment=1,
        fontName='Helvetica',
    )

    # Cell styles for issue detail tables
    cell_style = ParagraphStyle(
        'CellStyle', parent=styles['Normal'],
        fontSize=7, leading=9, fontName='Helvetica',
        textColor=text_dark,
    )
    cell_bold_style = ParagraphStyle(
        'CellBoldStyle', parent=cell_style,
        fontName='Helvetica-Bold',
    )

    # Timestamp
    ts = Table(
        [[Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", timestamp_style)]],
        colWidths=[7.5 * inch],
    )
    ts.setStyle(TableStyle([
        ('ALIGN', (0, 0), (0, 0), 'RIGHT'),
        ('TOPPADDING', (0, 0), (0, 0), -24),
        ('BOTTOMPADDING', (0, 0), (0, 0), 2),
    ]))
    story.append(ts)

    # Title
    title_text = f"{site_name + ' ' if site_name else ''}CLIF DQA Report Card"
    story.append(Paragraph(title_text, title_style))
    story.append(Paragraph(f"{table_name.title()} Table", heading_style))
    story.append(Spacer(1, 0.2 * inch))

    # --- Feedback banner (only when feedback with decisions exists) ---
    has_feedback = bool(feedback_lookup)
    if has_feedback:
        n_accepted = sum(1 for d in feedback_lookup.values() if d.get('decision') == 'accepted')
        n_rejected = sum(1 for d in feedback_lookup.values() if d.get('decision') == 'rejected')
        if n_accepted > 0 or n_rejected > 0:
            from datetime import datetime as _dt
            _raw_ts = feedback.get('timestamp', '')
            try:
                fb_ts = _dt.fromisoformat(_raw_ts).strftime('%Y-%m-%d %H:%M')
            except (ValueError, TypeError):
                fb_ts = _raw_ts
            banner_style = ParagraphStyle(
                'FeedbackBanner', parent=styles['Normal'],
                fontSize=8, textColor=colors.HexColor('#1F4E79'),
                fontName='Helvetica',
            )
            banner_text = (
                f"<i>This report was updated based on feedback provided on {fb_ts}. "
                f"Accepted: {n_accepted} | Rejected: {n_rejected}</i>"
            )
            banner_tbl = Table(
                [[Paragraph(banner_text, banner_style)]],
                colWidths=[7.5 * inch],
            )
            banner_tbl.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (0, 0), colors.HexColor('#E8F0FE')),
                ('TOPPADDING', (0, 0), (0, 0), 6),
                ('BOTTOMPADDING', (0, 0), (0, 0), 6),
                ('LEFTPADDING', (0, 0), (0, 0), 10),
                ('RIGHTPADDING', (0, 0), (0, 0), 10),
            ]))
            story.append(banner_tbl)
            story.append(Spacer(1, 0.15 * inch))

    # --- DQA Summary Table ---
    story.append(Paragraph("DQA Summary", heading_style))
    summary_header = ['Category', 'Non-Error', 'Total', 'Errors', 'Warnings']
    summary_rows = [summary_header]
    for category in DQA_CATEGORIES:
        if category not in category_scores:
            continue
        passed, total = category_scores[category]
        cat_issues = [i for i in all_issues if i['category'] == category]
        cat_errors = sum(1 for i in cat_issues if i['severity'] == 'error')
        cat_warnings = sum(1 for i in cat_issues if i['severity'] == 'warning')
        # Adjust for rejected errors in this category
        if rejected_ids:
            cat_rejected = sum(
                1 for i in cat_issues
                if i['severity'] == 'error' and _make_error_id(i) in rejected_ids
            )
            cat_errors -= cat_rejected
            passed += cat_rejected
        summary_rows.append([category.title(), str(passed), str(total),
                             str(cat_errors), str(cat_warnings)])
    summary_rows.append(['Overall', str(total_passed), str(total_checks),
                         str(error_count), str(warning_count)])

    summary_tbl = Table(summary_rows, colWidths=[2 * inch, 0.8 * inch, 0.8 * inch,
                                                  0.8 * inch, 0.8 * inch])
    tbl_style = [
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('BACKGROUND', (0, 0), (-1, 0), header_bg),
        ('TEXTCOLOR', (0, 0), (-1, -1), text_dark),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#DADADA')),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('LEFTPADDING', (0, 0), (-1, -1), 10),
        ('RIGHTPADDING', (0, 0), (-1, -1), 10),
    ]
    # Color-code error/warning cells
    for row_idx in range(1, len(summary_rows)):
        errors_val = int(summary_rows[row_idx][3])
        warnings_val = int(summary_rows[row_idx][4])
        if errors_val > 0:
            tbl_style.append(('BACKGROUND', (3, row_idx), (3, row_idx), fail_bg))
        else:
            tbl_style.append(('BACKGROUND', (3, row_idx), (3, row_idx), pass_bg))
        if warnings_val > 0:
            tbl_style.append(('BACKGROUND', (4, row_idx), (4, row_idx), warn_bg))
        else:
            tbl_style.append(('BACKGROUND', (4, row_idx), (4, row_idx), pass_bg))
    summary_tbl.setStyle(TableStyle(tbl_style))
    story.append(summary_tbl)
    story.append(Spacer(1, 0.3 * inch))

    # --- Data Profile Table ---
    table_stats = validation_data.get('table_stats', [])
    if table_stats:
        story.append(Paragraph("Data Profile", heading_style))
        total_rows = validation_data.get('total_rows', 0)
        profile_subtitle = ParagraphStyle(
            'ProfileSubtitle', parent=styles['Normal'],
            fontSize=9, textColor=text_medium, fontName='Helvetica',
        )
        story.append(Paragraph(f"Total Rows: {total_rows:,}", profile_subtitle))
        story.append(Spacer(1, 0.1 * inch))

        profile_header = ['Column', 'Dtype', 'Null', 'Null%', 'Unique']
        profile_rows = [profile_header]
        for s in table_stats:
            profile_rows.append([
                s['column'], s['dtype'],
                f"{s['null_count']:,}",
                f"{s['null_pct']:.1f}%", f"{s['unique']:,}",
            ])

        profile_col_widths = [2.0 * inch, 1.2 * inch,
                              0.8 * inch, 0.8 * inch, 0.8 * inch]
        profile_tbl = Table(profile_rows, colWidths=profile_col_widths)

        profile_style = [
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 7),
            ('BACKGROUND', (0, 0), (-1, 0), header_bg),
            ('TEXTCOLOR', (0, 0), (-1, -1), text_dark),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#DADADA')),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),
            # Right-align numeric columns (Non-Null, Null, Null%, Unique)
            ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),
        ]
        # Color-code Null% cells
        amber_bg = colors.HexColor('#FFF3E0')
        red_light_bg = colors.HexColor('#FFEAEA')
        for row_idx, s in enumerate(table_stats, 1):
            if s['null_pct'] > 50:
                profile_style.append(('BACKGROUND', (3, row_idx), (3, row_idx), red_light_bg))
            elif s['null_pct'] > 10:
                profile_style.append(('BACKGROUND', (3, row_idx), (3, row_idx), amber_bg))

        profile_tbl.setStyle(TableStyle(profile_style))
        story.append(profile_tbl)
        story.append(Spacer(1, 0.3 * inch))

    # --- Per-category issue details as structured tables ---
    rejected_bg = colors.HexColor('#E0E0E0')
    rejected_text = colors.HexColor('#999999')

    if all_issues:
        story.append(PageBreak())
        story.append(Paragraph(f"Issue Details ({len(all_issues)})", heading_style))

        # Column widths — add status column only when feedback exists
        if has_feedback:
            detail_col_widths = [1.0 * inch, 0.5 * inch, 1.7 * inch,
                                 1.1 * inch, 0.5 * inch, 2.2 * inch, 0.5 * inch]
        else:
            detail_col_widths = [1.0 * inch, 0.5 * inch, 1.7 * inch,
                                 1.1 * inch, 0.5 * inch, 2.7 * inch]

        for category in DQA_CATEGORIES:
            cat_issues = [i for i in all_issues if i['category'] == category]
            if not cat_issues:
                continue

            story.append(Paragraph(f"{category.title()} ({len(cat_issues)})", heading_style))

            # Header row
            header_row = [
                Paragraph('<b>metric</b>', cell_bold_style),
                Paragraph('<b>rule</b>', cell_bold_style),
                Paragraph('<b>rule_description</b>', cell_bold_style),
                Paragraph('<b>column_field</b>', cell_bold_style),
                Paragraph('<b>severity</b>', cell_bold_style),
                Paragraph('<b>finding</b>', cell_bold_style),
            ]
            if has_feedback:
                header_row.append(Paragraph('<b>status</b>', cell_bold_style))
            table_data = [header_row]

            for issue in cat_issues:
                severity_upper = issue['severity'].upper()
                finding_text = truncate_comment(issue.get('finding', issue['message']))
                # Build finding cell: text + optional sparkline for temporal checks
                spark_width = 2.0 * inch if has_feedback else 2.5 * inch
                yearly_counts = issue.get('details', {}).get('yearly_counts')
                if yearly_counts:
                    finding_cell = [
                        Paragraph(escape(finding_text), cell_style),
                        Spacer(1, 2),
                        YearlySparkBar(yearly_counts, width=spark_width, height=16),
                    ]
                else:
                    finding_cell = Paragraph(escape(finding_text), cell_style)
                row = [
                    Paragraph(escape(category), cell_style),
                    Paragraph(escape(issue.get('rule_code', '')), cell_style),
                    Paragraph(escape(issue.get('rule_description', '')), cell_style),
                    Paragraph(escape(issue.get('column_field', 'NA')), cell_style),
                    Paragraph(escape(severity_upper), cell_style),
                    finding_cell,
                ]
                if has_feedback:
                    error_id = _make_error_id(issue)
                    decision_info = feedback_lookup.get(error_id, {})
                    status_text = decision_info.get('decision', '').upper()
                    row.append(Paragraph(escape(status_text), cell_style))
                table_data.append(row)

            detail_tbl = Table(table_data, colWidths=detail_col_widths,
                               repeatRows=1)

            # Base table style
            detail_style = [
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 7),
                ('BACKGROUND', (0, 0), (-1, 0), header_bg),
                ('TEXTCOLOR', (0, 0), (-1, -1), text_dark),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#DADADA')),
                ('TOPPADDING', (0, 0), (-1, -1), 3),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
                ('LEFTPADDING', (0, 0), (-1, -1), 4),
                ('RIGHTPADDING', (0, 0), (-1, -1), 4),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ]

            # Color-code rows by severity, override with grey for rejected
            for row_idx, issue in enumerate(cat_issues, 1):
                error_id = _make_error_id(issue)
                decision = feedback_lookup.get(error_id, {}).get('decision', '')
                if decision == 'rejected':
                    detail_style.append(('BACKGROUND', (0, row_idx), (-1, row_idx), rejected_bg))
                    detail_style.append(('TEXTCOLOR', (0, row_idx), (-1, row_idx), rejected_text))
                elif issue['severity'] == 'error':
                    detail_style.append(('BACKGROUND', (0, row_idx), (-1, row_idx), fail_bg))
                elif issue['severity'] == 'warning':
                    detail_style.append(('BACKGROUND', (0, row_idx), (-1, row_idx), warn_bg))

            detail_tbl.setStyle(TableStyle(detail_style))
            story.append(detail_tbl)

            # Add note about monthly trend CSVs for temporal consistency checks
            temporal_cols = sorted({
                i['column_field'] for i in cat_issues
                if i.get('check_type') == 'category_temporal_consistency'
                and i.get('column_field')
            })
            if temporal_cols:
                file_list = ", ".join(
                    f"{table_name}_{col}_monthly.csv" for col in temporal_cols
                )
                note_text = (
                    f"<i>P.6 Category temporal consistency — monthly breakdown available at: "
                    f"clifpy/monthly_trends/ ({file_list})</i>"
                )
                note_style = ParagraphStyle(
                    'NoteStyle', parent=styles['Normal'],
                    fontSize=7, textColor=colors.HexColor('#555555'),
                )
                story.append(Paragraph(note_text, note_style))

            story.append(Spacer(1, 0.2 * inch))
    else:
        story.append(Paragraph("No validation issues found!", styles['Normal']))

    doc.build(story)
    return output_path


def generate_text_report(validation_data: Dict[str, Any],
                         table_name: str, output_path: str,
                         site_name: Optional[str] = None) -> str:
    """
    Generate a plain-text DQA report.

    Parameters match generate_validation_pdf.
    """
    category_scores, all_issues = collect_dqa_issues(validation_data)
    total_passed = sum(p for p, _ in category_scores.values())
    total_checks = sum(t for _, t in category_scores.values())
    error_count = sum(1 for i in all_issues if i['severity'] == 'error')
    warning_count = sum(1 for i in all_issues if i['severity'] == 'warning')

    lines = []
    lines.append("=" * 120)
    lines.append("CLIF 2.1 DQA VALIDATION REPORT")
    lines.append(f"{table_name.upper()} TABLE")
    lines.append("=" * 120)
    lines.append("")
    if site_name:
        lines.append(f"Site: {site_name}")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    # DQA Summary
    lines.append("-" * 120)
    lines.append("DQA SUMMARY")
    lines.append("-" * 120)
    lines.append(f"  {'Category':20s}  {'Passed':>6s}  {'Total':>5s}  {'Errors':>6s}  {'Warnings':>8s}")
    lines.append(f"  {'-'*20}  {'-'*6}  {'-'*5}  {'-'*6}  {'-'*8}")
    for category in DQA_CATEGORIES:
        if category not in category_scores:
            continue
        passed, total = category_scores[category]
        cat_issues = [i for i in all_issues if i['category'] == category]
        cat_errors = sum(1 for i in cat_issues if i['severity'] == 'error')
        cat_warnings = sum(1 for i in cat_issues if i['severity'] == 'warning')
        lines.append(f"  {category.title():20s}  {passed:>6d}  {total:>5d}  {cat_errors:>6d}  {cat_warnings:>8d}")
    lines.append(f"  {'Overall':20s}  {total_passed:>6d}  {total_checks:>5d}  {error_count:>6d}  {warning_count:>8d}")
    lines.append("")

    # Data Profile
    table_stats = validation_data.get('table_stats', [])
    if table_stats:
        lines.append("-" * 120)
        lines.append("DATA PROFILE")
        lines.append("-" * 120)
        total_rows = validation_data.get('total_rows', 0)
        lines.append(f"  Total Rows: {total_rows:,}")
        lines.append("")
        w_col, w_dtype, w_null, w_pct, w_uniq = 25, 12, 8, 8, 10
        hdr = (f"  {'Column':<{w_col}}"
               f"{'Dtype':<{w_dtype}}"
               f"{'Null':>{w_null}}"
               f"{'Null%':>{w_pct}}"
               f"{'Unique':>{w_uniq}}")
        lines.append(hdr)
        lines.append("  " + "-" * (w_col + w_dtype + w_null + w_pct + w_uniq))
        for s in table_stats:
            col_name = s['column']
            if len(col_name) > w_col - 2:
                col_name = col_name[:w_col - 4] + '..'
            lines.append(
                f"  {col_name:<{w_col}}"
                f"{s['dtype']:<{w_dtype}}"
                f"{s['null_count']:>{w_null},}"
                f"{s['null_pct']:>{w_pct}.1f}%"
                f"{s['unique']:>{w_uniq},}"
            )
        lines.append("")

    # Issue details as tabular text
    if all_issues:
        lines.append("=" * 120)
        lines.append(f"ISSUE DETAILS ({len(all_issues)})")
        lines.append("=" * 120)

        # Column widths for text alignment
        w_metric, w_rule, w_desc, w_col, w_sev = 14, 6, 30, 18, 10

        for category in DQA_CATEGORIES:
            cat_issues = [i for i in all_issues if i['category'] == category]
            if not cat_issues:
                continue

            lines.append("")
            lines.append(f"-- {category.title()} ({len(cat_issues)}) --")
            lines.append("")

            # Header
            hdr = (f"  {'metric':<{w_metric}}"
                   f"{'rule':<{w_rule}}"
                   f"{'rule_description':<{w_desc}}"
                   f"{'column_field':<{w_col}}"
                   f"{'severity':<{w_sev}}"
                   f"finding")
            lines.append(hdr)
            lines.append("  " + "-" * 116)

            for issue in cat_issues:
                severity_upper = issue['severity'].upper()
                rule_code = issue.get('rule_code', '')
                rule_desc = issue.get('rule_description', '')
                col_field = issue.get('column_field', 'NA')
                finding = issue.get('finding', issue['message'])

                # Truncate long fields for text display
                if len(rule_desc) > w_desc - 2:
                    rule_desc = rule_desc[:w_desc - 4] + '..'
                if len(col_field) > w_col - 2:
                    col_field = col_field[:w_col - 4] + '..'

                line = (f"  {category:<{w_metric}}"
                        f"{rule_code:<{w_rule}}"
                        f"{rule_desc:<{w_desc}}"
                        f"{col_field:<{w_col}}"
                        f"{severity_upper:<{w_sev}}"
                        f"{finding}")
                lines.append(line)
    else:
        lines.append("No validation issues found!")

    lines.append("")
    lines.append("=" * 120)
    lines.append("END OF REPORT")
    lines.append("=" * 120)

    with open(output_path, 'w') as f:
        f.write('\n'.join(lines))

    return output_path
