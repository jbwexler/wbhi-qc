#!/usr/bin/env python

import flywheel_gear_toolkit
import flywheel
from flywheel import ProjectOutput, SessionListOutput
import logging
import pandas as pd
from datetime import datetime
from bidsmosaic.mosaic import create_mosaic_pdf


log = logging.getLogger(__name__)


def create_view_df(
    container, column_dict: dict, client, filter=None, container_type="acquisition"
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
    for c in column_dict:
        builder.column(src=c)

    view = builder.build()
    return client.read_view_dataframe(view, container.id, opts={"dtype": column_dict})


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
        "subject.id",
    ]
    file_df = create_view_df(
        project,
        columns,
        client,
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
        file_df.to_csv(f)

    unique_df = file_df.drop_duplicates(
        subset=["file.info.header.dicom.SeriesDescription", "no_sub_bids_filename"]
    )
    unique_df.insert(0, "notes", "")
    unique_df.insert(1, "action", "")

    with gtk_context.open_output(f"wbhi-qc_{today}_unique.csv", "w") as f:
        unique_df.to_csv(f)

    log.info("Successfully created csv")


def process_csv_input(
    csv_input: str, all_df: pd.DataFrame, group_id: str
) -> pd.DataFrame:
    """Extrapolates "action" and "notes" columns to all rows in all_df."""
    all_df = all_df.copy().fillna("")
    csv_df = pd.read_csv(csv_input).fillna("")

    csv_df["action"] = csv_df["action"].str.lower()

    match_columns = ["file.info.header.dicom.SeriesDescription", "no_sub_bids_filename"]
    merge_columns = match_columns + ["notes", "action"]

    return all_df.merge(csv_df[merge_columns], on=match_columns, how="inner")


def mv_untag_subs(all_df: pd.DataFrame, group_id: str) -> None:
    """Moves all subjects containing only "good" files from "staging" to "upload" project.
    The remaining sessions have their 'bidsified' tag removed."""
    all_df = all_df.copy()
    sub_s = all_df.groupby("subject.id")["action"].apply(
        lambda x: "move" if (x == "good").all() else "untag"
    )
    upload_project_path = f"{group_id}/upload"
    upload_project = client.lookup(upload_project_path)

    for sub_id, sub_action in sub_s.items():
        sub = client.get_subject(sub_id)
        sessions = sub.sessions()

        if sub_action == "move":
            for ses in sessions:
                if ses.project == upload_project.id:
                    log.warning(
                        "Session %s/%s already in %s."
                        % (sub.label, ses.label, upload_project_path)
                    )
                    continue

                log.info(
                    "Moving session %s/%s to %s."
                    % (sub.label, ses.label, upload_project_path)
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
    rm_df = all_df.copy()
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
    fix_df = all_df.copy()
    fix_df = all_df[all_df["action"] == "fix"]
    notes_col = fix_df.pop("notes")
    fix_df.insert(0, "notes", notes_col)
    fix_df = fix_df.sort_values(["notes", "subject.label", "acquisition.timestamp"])

    today = datetime.today().date().strftime("%Y%m%d")
    fix_csv_name = f"wbhi-qc_{today}_fix.csv"

    log.info("Creating %s." % fix_csv_name)
    with gtk_context.open_output(fix_csv_name, "w") as f:
        fix_df.to_csv(f, index=False)


def main():
    gtk_context.init_logging()
    gtk_context.log_config()

    destination_id = gtk_context.destination["id"]
    project_id = client.get(destination_id)["parents"]["project"]
    group_id = client.get(destination_id)["parents"]["group"]
    project = client.get_project(project_id)

    csv_input = gtk_context.get_input_path("unique_csv")
    if csv_input:
        all_df = create_file_csv(project, dryrun=True)
        all_df = process_csv_input(csv_input, all_df, group_id)

        mv_untag_subs(all_df, group_id)
        rename_remove_files(all_df, project)
        create_fix_csv(all_df)
    else:
        create_file_csv(project)
        bids_mosaic()


if __name__ == "__main__":
    with flywheel_gear_toolkit.GearToolkitContext() as gtk_context:
        config = gtk_context.config
        client = gtk_context.client

        main()
