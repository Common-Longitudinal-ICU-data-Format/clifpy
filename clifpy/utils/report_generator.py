"""PDF and text report generator for DQA validation results."""

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

    def wrap(self, availWidth, availHeight):
        return self.width, self.height

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
        for i, year in enumerate(years):
            count = self.yearly_counts[year]
            x = i * (bar_w + gap)
            if count > 0:
                bar_h = max(2, (count / max_count) * self.height)
                self.canv.setFillColor(present_color)
            else:
                bar_h = self.height  # full height red bar for absent
                self.canv.setFillColor(absent_color)
            self.canv.rect(x, 0, bar_w, bar_h, stroke=0, fill=1)

DQA_CATEGORIES = ('conformance', 'completeness', 'plausibility')


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
            cat_passed = sum(1 for i in cat_issues if i['severity'] == 'info')
            category_scores[category] = (cat_passed, len(cat_issues))

    return category_scores, all_issues


def generate_validation_pdf(validation_data: Dict[str, Any],
                            table_name: str, output_path: str,
                            site_name: Optional[str] = None) -> str:
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

    Returns
    -------
    str
        Path to generated PDF file.
    """
    category_scores, all_issues = collect_dqa_issues(validation_data)
    total_passed = sum(p for p, _ in category_scores.values())
    total_checks = sum(t for _, t in category_scores.values())
    error_count = sum(1 for i in all_issues if i['severity'] == 'error')
    warning_count = sum(1 for i in all_issues if i['severity'] == 'warning')

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

    # --- DQA Summary Table ---
    story.append(Paragraph("DQA Summary", heading_style))
    summary_header = ['Category', 'Passed', 'Total', 'Errors', 'Warnings']
    summary_rows = [summary_header]
    for category in DQA_CATEGORIES:
        if category not in category_scores:
            continue
        passed, total = category_scores[category]
        cat_issues = [i for i in all_issues if i['category'] == category]
        cat_errors = sum(1 for i in cat_issues if i['severity'] == 'error')
        cat_warnings = sum(1 for i in cat_issues if i['severity'] == 'warning')
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

    # --- Per-category issue details as structured tables ---
    if all_issues:
        story.append(PageBreak())
        story.append(Paragraph(f"Issue Details ({len(all_issues)})", heading_style))

        # Column widths: metric(1.0") rule(0.5") rule_desc(1.7") column_field(1.1") severity(0.5") finding(2.7")
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
            table_data = [header_row]

            for issue in cat_issues:
                severity_upper = issue['severity'].upper()
                finding_text = truncate_comment(issue.get('finding', issue['message']))
                # Build finding cell: text + optional sparkline for temporal checks
                yearly_counts = issue.get('details', {}).get('yearly_counts')
                if yearly_counts:
                    finding_cell = [
                        Paragraph(escape(finding_text), cell_style),
                        Spacer(1, 2),
                        YearlySparkBar(yearly_counts, width=2.5 * inch, height=16),
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

            # Color-code rows by severity
            for row_idx, issue in enumerate(cat_issues, 1):
                if issue['severity'] == 'error':
                    detail_style.append(('BACKGROUND', (0, row_idx), (-1, row_idx), fail_bg))
                elif issue['severity'] == 'warning':
                    detail_style.append(('BACKGROUND', (0, row_idx), (-1, row_idx), warn_bg))

            detail_tbl.setStyle(TableStyle(detail_style))
            story.append(detail_tbl)
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
