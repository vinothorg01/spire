"""Test processing of Conde Nast data to generate targets."""
from spire.targets import CondeNastConfig, TargetsRunner


def test_condenast_pt_transform(
    ga_ids_full_fixture,
    covid_pt_transform_fixture,
    affiliate_pt_transform_fixture,
    date_fixture,
):
    """Test pre-processing of Conde Nast Passthrough data."""
    condenast_pt = CondeNastConfig(date_fixture)
    condenast_pt_runner = TargetsRunner(condenast_pt)

    transform_output = condenast_pt_runner.transform(ga_ids_full_fixture)

    num_diff_output_fixture = transform_output.exceptAll(
        covid_pt_transform_fixture.union(affiliate_pt_transform_fixture)
    ).count()
    assert num_diff_output_fixture == 0


def test_condenast_map_schema(
    covid_pt_transform_fixture, affiliate_pt_transform_fixture, date_fixture
):
    """
    Test mapping of processed data to targets schema.

    Unlike regular targets, the Passthrough targets do Spire Targets normalization
    within the transform method. This test just ensures that the output does not break
    and is identical to map_schema since it still gets processed through map_schema via
    the TargetsRunner
    """
    condenast_pt = CondeNastConfig(date_fixture)
    condenast_pt_runner = TargetsRunner(condenast_pt)

    # CondeNastConfig transform unionizes the transformed data of CovidPT and
    # AffiliatesPT, so the fixtures are also unionized.
    condenast_pt_transform_fixture = covid_pt_transform_fixture.union(
        affiliate_pt_transform_fixture
    )

    targets_output = condenast_pt_runner.map_schema(condenast_pt_transform_fixture)

    num_diff_output_fixture = targets_output.exceptAll(
        condenast_pt_transform_fixture
    ).count()
    assert num_diff_output_fixture == 0
