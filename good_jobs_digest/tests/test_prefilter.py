from rank.prefilter import prefilter_title

INCLUDE = [
    "artificial intelligence engineer",
    "analytics engineer",
    "analytics engineering",
    "ai engineer",
    "machine learning engineer",
    "ml engineer",
    "data engineer",
    "data engineering",
    "data integration",
    "data integrations",
    "data platform engineer",
    "etl engineer",
]
EXCLUDE = [
    "intern",
    "internship",
]


def test_prefilter_accepts_data_engineer():
    assert prefilter_title(
        "Senior Data Engineer",
        include_keywords=INCLUDE,
        exclude_keywords=EXCLUDE,
    )


def test_prefilter_accepts_analytics_engineer():
    assert prefilter_title(
        "Staff Analytics Engineer",
        include_keywords=INCLUDE,
        exclude_keywords=EXCLUDE,
    )


def test_prefilter_accepts_data_integration():
    assert prefilter_title(
        "Lead, Data Integrations",
        include_keywords=INCLUDE,
        exclude_keywords=EXCLUDE,
    )


def test_prefilter_rejects_intern():
    assert not prefilter_title(
        "Summer intern, data team",
        include_keywords=INCLUDE,
        exclude_keywords=EXCLUDE,
    )


def test_prefilter_rejects_data_engineer_intern():
    assert not prefilter_title(
        "Data Engineer Intern",
        include_keywords=INCLUDE,
        exclude_keywords=EXCLUDE,
    )


def test_prefilter_accepts_full_stack_analytics_engineer_without_broad_exclude():
    """Broad excludes like 'full stack' drop compound titles that still match target roles."""
    assert prefilter_title(
        "Full Stack Analytics Engineer",
        include_keywords=INCLUDE,
        exclude_keywords=EXCLUDE,
    )


def test_prefilter_rejects_generic_engineer():
    assert not prefilter_title(
        "Senior Engineer",
        include_keywords=INCLUDE,
        exclude_keywords=EXCLUDE,
    )


def test_prefilter_requires_keyword():
    assert not prefilter_title(
        "Office Manager",
        include_keywords=INCLUDE,
        exclude_keywords=EXCLUDE,
    )
