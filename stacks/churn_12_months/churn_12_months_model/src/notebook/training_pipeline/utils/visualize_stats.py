from kfp.v2.dsl import (Artifact, Output, Input, HTML, component)

# Visualize Stats Component: monitoring component for visualizing statistics created by generate_stats component
@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-tfdv-slim:1.0.0",
    output_component_file="visualize_stats.yaml"
)
def visualize_stats(
    statistics: Input[Artifact],
    view: Output[HTML],
    op_type: str = "",
    stats_nm: str = "",
    base_stats_path: str = None,
    base_stats_nm: str = ""
):
    '''
    Inputs:
        - op_type: training or serving or predictions
        - stats_nm: name of new stats
        - base_stats_path: path to base stats in gcs (usually training)
        - base_stats_nm: base stats name
        - statistics: path to statistics imported from generate stats component
    
    Outputs:
        - html artifact
    '''

    import tensorflow_data_validation as tfdv
    from tensorflow_data_validation.utils.display_util import (
        get_statistics_html,
    )

    # load stats
    stats = tfdv.load_statistics(input_path=statistics.uri)
    print("statistics uri")
    print(statistics.uri)

    # create html content
    if base_stats_path is not None:
        base_stats = tfdv.load_statistics(input_path=base_stats_path)

        html = get_statistics_html(
            lhs_statistics=stats,
            lhs_name=stats_nm,
            rhs_statistics=base_stats,
            rhs_name=base_stats_nm,
        )
    
    else:
        html = get_statistics_html(
            lhs_statistics=stats,
            lhs_name=stats_nm,
        )

    # ensure view is stored as html (this will set content-type to text/html)
    if not view.path.endswith(".html"):
        view.path += ".html"

    print("view path")
    print(view.path)

    # write html to output file
    with open(view.path, "w") as f:
        f.write(html)