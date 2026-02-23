#!/usr/bin/env python

import flywheel_gear_toolkit
import flywheel
from flywheel import ProjectOutput
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


def bids_mosaic() -> None:
    """Creates a bids-mosaic pdf."""
    bids_path = gtk_context.download_project_bids(folders=['anat'])
    today = datetime.today().date().strftime("%Y%m%d")

    with gtk_context.open_output(f"wbhi-qc_{today}.pdf", 'wb') as f:
        create_mosaic_pdf(
            bids_path,
            f,
            anat=True,
            png_out_dir=None,
            downsample=None,
            freesurfer=None,
            metadata=None,
        )


def create_file_csv(project: ProjectOutput) -> None:
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
    ]
    file_df = create_view_df(
        project,
        columns,
        client,
        filter="file.type=nifti",
    )
    
    today = datetime.today().date().strftime("%Y%m%d")
    with gtk_context.open_output(f"wbhi-qc_{today}_all.csv", 'w') as f:
        file_df.to_csv(f)

    file_df.loc[:, "no_sub_bids_filename"] = file_df["file.info.BIDS.Filename"].fillna("").apply(lambda x: x.split('_', maxsplit=1)[1] if x else x)
    unique_df = file_df.drop_duplicates(subset=["file.info.header.dicom.SeriesDescription", "no_sub_bids_filename"])
    del unique_df["no_sub_bids_filename"]

    with gtk_context.open_output(f"wbhi-qc_{today}_unique.csv", 'w') as f:
        unique_df.to_csv(f)

    log.info("Successfully created csv")




def main():
    gtk_context.init_logging()
    gtk_context.log_config()


    destination_id = gtk_context.destination["id"]
    project_id = client.get(destination_id)["parents"]["project"]
    project = client.get_project(project_id)

    bids_mosaic()
    create_file_csv(project)


if __name__ == "__main__":
    with flywheel_gear_toolkit.GearToolkitContext() as gtk_context:
        config = gtk_context.config
        client = gtk_context.client

        main()
