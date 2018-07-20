from jinja2 import contextfilter


@contextfilter
def _render(context, value):
    """
    jinja2 context filter that lets you render a template string within a
    template. See https://stackoverflow.com/a/51104298/1483986
    """
    ti = context['ti']
    return ti.task.render_template("", value, ti.get_template_context())
