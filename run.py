#!/usr/bin/env python

import flywheel_gear_toolkit
import flywheel
from flywheel import ProjectOutput, SessionListOutput
import logging
import sys
import pandas as pd
from datetime import datetime
from bidsmosaic.mosaic import create_mosaic_pdf


log = logging.getLogger(__name__)


def create_view_df(
    container, column_list: list, filter=None, container_type="acquisition"
):
    """Get unique labels for all acquisitions in the container.

    This is done using a single Data View which is more efficient than iterating through
    all acquisitions, sessions, and subjects. This prevents time-out errors in large projects.
    """

    builder = flywheel.ViewBuilder(
        container=container_type,
        filename="*.*",
        match="all",
        filter=filter,
        process_files=False,
        include_ids=False,
        include_labels=False,
    )
    for c in column_list:
        builder.column(src=c)

    view = builder.build()
    return client.read_view_dataframe(view, container.id)


def mv_session(session: SessionListOutput, dst_project: ProjectOutput) -> None:
    """Moves a session to another project."""
    try:
        session.update(project=dst_project.id)
    except flywheel.ApiException as exc:
        if exc.status == 422:
            sub_label = client.get_subject(session.parents.subject).label.replace(
                ",", r"\,"
            )
            subject_dst_id = dst_project.subjects.find_first(f'label="{sub_label}"').id
            body = {
                "sources": [session.id],
                "destinations": [subject_dst_id],
                "destination_container_type": "subjects",
                "conflict_mode": "skip",
            }
            client.bulk_move_sessions(body=body)
        else:
            log.exception(
                "Error moving subject %s from %s to %s",
                session.subject.label,
                session.id,
                dst_project.label,
            )


def bids_mosaic() -> None:
    """Creates a bids-mosaic pdf."""
    bids_path = gtk_context.download_project_bids(folders=["anat"])
    today = datetime.today().date().strftime("%Y%m%d")

    with gtk_context.open_output(f"wbhi-qc_{today}.pdf", "wb") as f:
        create_mosaic_pdf(
            bids_path,
            f,
            anat=True,
            png_out_dir=None,
            downsample=None,
            freesurfer=None,
            metadata=None,
        )


def create_file_csv(project: ProjectOutput, dryrun=False) -> None:
    """Create a csv with information about each unique acquisition.label and
    bids.label pair."""
    columns = [
        "file.info.header.dicom.SeriesDescription",
        "file.info.BIDS.Filename",
        "acquisition.label",
        "subject.label",
        "session.label",
        "acquisition.id",
        "file.info.header.dicom.ImageType",
        "file.info.header.dicom_array.ImageType.0",
        "file.classification.Intent",
        "file.classification.Measurement",
        "file.classification.Features",
        "file.modality",
        "file.created",
        "file.name",
        "acquisition.timestamp",
        "session.id",
        "subject.label",
    ]
    file_df = create_view_df(
        project,
        columns,
        filter="file.type=nifti",
    )

    if file_df.empty:
        return file_df

    file_df.loc[:, "no_sub_bids_filename"] = (
        file_df["file.info.BIDS.Filename"]
        .fillna("")
        .apply(lambda x: x.split("_", maxsplit=1)[1] if x else x)
    )

    if dryrun:
        return file_df

    today = datetime.today().date().strftime("%Y%m%d")
    with gtk_context.open_output(f"wbhi-qc_{today}_all.csv", "w") as f:
        file_df.to_csv(f, index=False)

    unique_df = file_df.drop_duplicates(
        subset=["file.info.header.dicom.SeriesDescription", "no_sub_bids_filename"]
    )
    unique_df.insert(0, "notes", "")
    unique_df.insert(1, "action", "")

    with gtk_context.open_output(f"wbhi-qc_{today}_unique.csv", "w") as f:
        unique_df.to_csv(f, index=False)

    log.info("Successfully created csv")


def process_csv_input(unique_csv_input: str, all_csv_input: str) -> pd.DataFrame:
    """Extrapolates "action" and "notes" columns to all rows in all_df."""
    unique_df = (
        pd.read_csv(unique_csv_input)
        .drop(columns=["Unnamed: 0"], errors="ignore")
        .fillna("")
    )
    all_df = (
        pd.read_csv(all_csv_input)
        .drop(columns=["Unnamed: 0"], errors="ignore")
        .fillna("")
    )

    unique_df["action"] = unique_df["action"].str.lower()

    match_columns = ["file.info.header.dicom.SeriesDescription", "no_sub_bids_filename"]
    merge_columns = match_columns + ["notes", "action"]

    return all_df.merge(unique_df[merge_columns], on=match_columns, how="inner")


def mv_untag_subs(all_df: pd.DataFrame, upload_project: ProjectOutput) -> None:
    """Moves all subjects containing only "good" files from "staging" to "upload" project.
    The remaining sessions have their 'bidsified' tag removed."""
    all_df = all_df.copy()
    sub_s = all_df.groupby("subject.label")["action"].apply(
        lambda x: "move" if (x == "good").all() else "untag"
    )

    for sub_id, sub_action in sub_s.items():
        sub = client.get_subject(sub_id)
        sessions = sub.sessions()

        if sub_action == "move":
            for ses in sessions:
                if ses.project == upload_project.id:
                    log.warning(
                        "Session %s/%s already in %s."
                        % (sub.label, ses.label, upload_project.label)
                    )
                    continue

                log.info(
                    "Moving session %s/%s to %s."
                    % (sub.label, ses.label, upload_project.label)
                )
                mv_session(ses, upload_project)
        else:
            for ses in sessions:
                if "bidsified" not in ses.tags:
                    log.warning("'bidsified' tag not in %s/%s" % (sub.label, ses.label))
                    continue

                log.info("Removing 'bidsified' tag for %s/%s" % (sub.label, ses.label))
                ses.delete_tag("bidsified")


def rename_remove_files(all_df: pd.DataFrame, project: ProjectOutput) -> None:
    """Add "_ignore-BIDS" suffix to all "remove" files."""
    rm_df = all_df[all_df["action"] == "remove"]
    acq_s = rm_df["acquisition.id"].drop_duplicates()

    for acq_id in acq_s:
        acq = client.get_acquisition(acq_id)
        label = acq.label

        if label.endswith("_ignore-BIDS"):
            log.warning(
                "Acquisition %s/%s already ends with '_ignore-BIDS'" % (acq_id, label)
            )
            continue

        new_label = f"{label}_ignore-BIDS"
        log.info("Renaming acquisition %s from %s to %s" % (acq_id, label, new_label))
        acq.update({"label": new_label})


def create_fix_csv(all_df: pd.DataFrame) -> None:
    """Creates a csv containing all files that need to be fixed."""
    fix_df = all_df[all_df["action"] == "fix"]
    notes_col = fix_df.pop("notes")
    fix_df.insert(0, "notes", notes_col)
    fix_df = fix_df.sort_values(["notes", "subject.label", "acquisition.timestamp"])

    today = datetime.today().date().strftime("%Y%m%d")
    fix_csv_name = f"wbhi-qc_{today}_fix.csv"

    log.info("Creating %s." % fix_csv_name)
    with gtk_context.open_output(fix_csv_name, "w") as f:
        fix_df.to_csv(f, index=False)


def create_diff_csv(
    all_input_df: pd.DataFrame,
    all_current_df: pd.DataFrame,
    upload_project: ProjectOutput,
) -> None:
    """Creates csv diffing unique_csv input against current state."""
    upload_df = create_view_df(upload_project, ["acquisition.id"])
    upload_ids = set(upload_df["acquisition.id"])

    input_acq_df = all_input_df.drop_duplicates(subset="acquisition.id").rename(
        columns={"acquisition.label": "acq_label_orig"}
    )
    current_acq_df = (
        all_current_df[["acquisition.id", "acquisition.label"]]
        .drop_duplicates(subset="acquisition.id")
        .rename(columns={"acquisition.label": "acq_label_current"})
    )

    merged_df = input_acq_df.merge(current_acq_df, on="acquisition.id", how="outer")

    def get_change(row):
        if pd.isna(row["acq_label_orig"]):
            return "new"
        if pd.isna(row["acq_label_current"]):
            return (
                "moved_to_upload" if row["acquisition.id"] in upload_ids else "deleted"
            )
        if row["acq_label_current"] == f"{row['acq_label_orig']}_ignore-BIDS":
            return "removed"
        if row["acq_label_orig"] != row["acq_label_current"]:
            return "acq_label"
        return "unchanged"

    merged_df["change"] = merged_df.apply(get_change, axis=1)

    all_good_subject_ids = set(
        merged_df.groupby("subject.label").filter(
            lambda x: (x["action"].fillna("") == "good").all()
        )["subject.label"]
    )

    expected_changes = {
        "remove": "removed",
        "fix": "acq_label",
    }

    def get_error(row):
        action = row.get("action", "")
        if pd.isna(action) or action == "":
            return False
        if action == "good":
            expected = (
                "moved_to_upload"
                if row["subject.label"] in all_good_subject_ids
                else "unchanged"
            )
        else:
            expected = expected_changes.get(action)
        return expected is not None and row["change"] != expected

    merged_df["error"] = merged_df.apply(get_error, axis=1)

    priority_cols = [
        "subject.label",
        "acq_label_orig",
        "acq_label_current",
        "change",
        "error",
        "action",
        "notes",
        "no_sub_bids_filename",
    ]
    remaining_cols = [c for c in merged_df.columns if c not in priority_cols]
    merged_df = merged_df[priority_cols + remaining_cols]
    merged_df = merged_df.sort_values("error", ascending=False)

    today = datetime.today().date().strftime("%Y%m%d")
    diff_csv_name = f"wbhi-qc_{today}_diff.csv"
    log.info("Creating %s." % diff_csv_name)
    with gtk_context.open_output(diff_csv_name, "w") as f:
        merged_df.to_csv(f, index=False)


def main():
    gtk_context.init_logging()
    gtk_context.log_config()

    destination_id = gtk_context.destination["id"]
    project_id = client.get(destination_id)["parents"]["project"]
    group_id = client.get(destination_id)["parents"]["group"]
    project = client.get_project(project_id)
    upload_project_path = f"{group_id}/upload"
    upload_project = client.lookup(upload_project_path)

    unique_csv_input = gtk_context.get_input_path("unique_csv")
    all_csv_input = gtk_context.get_input_path("all_csv")
    if bool(unique_csv_input) != bool(all_csv_input):
        log.error("unique_csv and all_csv must be included together.")
        sys.exit(1)

    mode = config["mode"]

    if mode in ("apply", "diff"):
        if not unique_csv_input or not all_csv_input:
            log.error("unique_csv and all_csv are required for mode '%s'." % mode)
            sys.exit(1)
        all_input_df = process_csv_input(unique_csv_input, all_csv_input)

        if mode == "diff":
            all_current_df = create_file_csv(project, dryrun=True)
            create_diff_csv(all_input_df, all_current_df, upload_project)
        else:
            mv_untag_subs(all_input_df, upload_project)
            rename_remove_files(all_input_df, project)
            create_fix_csv(all_input_df)
    else:
        create_file_csv(project)
        bids_mosaic()


if __name__ == "__main__":
    with flywheel_gear_toolkit.GearToolkitContext() as gtk_context:
        config = gtk_context.config
        client = gtk_context.client

        main()
