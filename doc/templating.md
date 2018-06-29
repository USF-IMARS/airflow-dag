Many operator parameters function as jinja templates.

Below are some lines I used to explore what is available in the template
context:

```bash
# template "self":
# ----
# {{ self }} &&
# {{ self.__dir__ }} &&
# {{ self._TemplateReference__context }} &&
# {{ self.__dict__['_TemplateReference__context'] }} &&
# {{ self.__dict__['_TemplateReference__context']['GEOFILE'] }} &&
# {{ self.__dict__['_TemplateReference__context']['ti'] }}
# {{ self.__dict__['_TemplateReference__context']['ti'].task.render_template('', MYD01FILE,{"ts_nodash": ts_nodash}) }}
#
# ti:
# ----
# {{ ti }}
# {{ ti.jobid }}
# {{ ti.__dir__() }}
# {{ ti.task.render_template("", MYD01FILE, {"ts_nodash":ts_nodash}) }}
# {{ ti.task.render_template("", MYD01FILE, ti.get_template_context()) }}
# {{ ti.task.render_template("", MYD01FILE, self._TemplateReference__context) }} &&
# {{ MYD01FILE|render }} &&
#
# context:
# ----
# {{ti.get_template_context()}}
```
