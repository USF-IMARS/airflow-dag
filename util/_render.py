from jinja2 import contextfilter


@contextfilter
def _render(context, value):
    ti = context['ti']
    return ti.task.render_template("", value, ti.get_template_context())
