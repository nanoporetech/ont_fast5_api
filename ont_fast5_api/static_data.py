# Current workflows add the component name to the hdf group as
# an attribute. For older files, the following dict allows us
# to work out the component name from the group name.
LEGACY_COMPONENT_NAMES = {'Alignment': 'alignment',
                          'Basecall_1D': 'basecall_1d',
                          'OnlineBasecall': 'basecall_1d',
                          'Basecall_2D': 'basecall_2d',
                          'Calibration_Strand': 'calibration_strand',
                          'EventDetection': 'event_detection',
                          'Segmentation': 'segmentation',
                          'Hairpin_Split': 'segmentation',
                          'Segment_Linear': 'segmentation',
                          'Validation': 'segmentation',
                          'AlignToRef': 'align_to_ref',
                          'Barcoding': 'barcoding',
                          'Classification': 'classification',
                          'Evaluation': 'evaluation',
                          'Multiple_Alignment': 'multiple_alignment',
                          'Squiggle_Map': 'squiggle_map',
                          'Sam_Segmentor': 'sam_segmentor',
                          'arma': 'arma',
                          'Basic_component': 'basic_component',
                          }

supported_modes = ('r', 'r+', 'w', 'w-', 'x', 'a')
mode_docstring = """Supported file modes:
    r        Readonly, file must exist (default)
    r+       Read/write, file must exist
    w        Create file, truncate if exists
    w- or x  Create file, fail if exists
    a        Read/write if exists, create otherwise"""  # Taken from h5py

HARDLINK_GROUPS = ("context_tags", "tracking_id")
OPTIONAL_READ_GROUPS = {'Analyses'}
