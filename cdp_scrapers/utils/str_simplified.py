import cleantext


def str_simplified(input_str: str) -> str:
    """
    Remove leading and trailing whitespaces, simplify multiple whitespaces, unify
    newline characters.

    Parameters
    ----------
    input_str: str

    Returns
    -------
    cleaned: str
        input_str stripped if it is a string
    """
    if not isinstance(input_str, str):
        return input_str

    input_str = cleantext.clean(
        input_str, fix_unicode=True, lower=False, to_ascii=False
    )

    # Replace utf-8 char encodings with single utf-8 chars themselves
    input_str = input_str.encode("utf-8").decode("utf-8")

    return input_str