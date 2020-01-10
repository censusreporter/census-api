import re
from flask import request
from werkzeug.exceptions import BadRequest
from functools import wraps


class QueryArgs(dict):
    __getattr__ = dict.get


class ValidationException(Exception):
    pass


class ClientRequestValidationException(BadRequest):
    def __init__(self, message, errors):
        super(ClientRequestValidationException, self).__init__(message)
        self.errors = errors


class Validation(object):
    def validate(self, raw):
        raise NotImplementedError

    def help_text(self):
        raise NotImplementedError


class Regex(Validation):
    def __init__(self, regex, help=None):
        self._help = help

        if not hasattr(regex, "match"):
            self._regex = regex
        else:
            self._regex = re.compile(regex)

    def validate(self, raw):
        if not raw:
            raise ValidationException()

        if self._regex and not self._regex.match(raw):
            raise ValidationException()

        return raw

    def help_text(self):
        return self._help if self._help else "A string that matches %s" % self._regex


class NonemptyString(Validation):
    def validate(self, raw):
        if raw:
            return raw
        else:
            raise ValidationException()

    def help_text(self):
        return "Non empty string"


class FloatRange(Validation):
    def __init__(self, minimum=-float('inf'), maximum=+float('inf')):
        self._min = minimum
        self._max = maximum

    def validate(self, raw):
        try:
            flot = float(raw)
            if self._min <= flot <= self._max:
                return flot
            else:
                raise ValidationException()
        except ValueError:
            raise ValidationException()

    def help_text(self):
        return "A float between %s and %s" % (self._min, self._max)


class IntegerRange(Validation):
    def __init__(self, minimum=0, maximum=15):
        self._min = minimum
        self._max = maximum

    def validate(self, raw):
        try:
            val = int(raw)
            if self._min <= val <= self._max:
                return val
            else:
                raise ValidationException()
        except ValueError:
            raise ValidationException()

    def help_text(self):
        return "An integer between %s and %s" % (self._min, self._max)


class StringList(Validation):
    def __init__(self, sep=',', item_validator=NonemptyString()):
        self._sep = sep
        self._item_validator = item_validator

    def validate(self, raw):
        return [self._item_validator.validate(r) for r in raw.split(self._sep)]

    def help_text(self):
        return "A list of strings separated by %s" % (self._sep)


class Bool(Validation):
    def validate(self, raw):
        try:
            res = bool(raw)
        except ValueError:
            res = False
        return res

    def help_text(self):
        return "A boolean like 'true' or 'false'"


class Integer(Validation):
    def validate(self, raw):
        try:
            res = int(raw)
        except ValueError:
            res = False
        return res

    def help_text(self):
        return "An integer like '1' or '123'"


class OneOf(Validation):
    def __init__(self, collection):
        self._collection = collection

    def validate(self, raw):
        if raw in self._collection:
            return raw
        else:
            raise ValidationException()

    def help_text(self):
        return "One of %s" % ', '.join(self._collection)


def qwarg_validate(validators):
    def validate(f):
        @wraps(f)
        def validate_qwargs(*args, **kwargs):
            if not request.args:
                request.args = {}
            errors = {}
            qwargs = QueryArgs()

            for name, value in request.args.items():
                if name not in validators:
                    qwargs[name] = value

            for (name, validator_dict) in validators.items():
                validation = validator_dict.get('valid')
                if not validation:
                    raise ValueError('You must specify a validator for %s' % name)

                required = validator_dict.get('required', False)

                if name not in request.args:
                    if required:
                        errors[name] = {
                            "raw": None,
                            "error": validation.help_text()
                        }
                    else:
                        if 'default' in validator_dict:
                            qwargs[name] = validator_dict['default']
                else:
                    try:
                        raw_value = request.args[name]
                        value = validation.validate(raw_value)
                        qwargs[name] = value
                    except ValidationException:
                        errors[name] = {
                            "raw": raw_value,
                            "error": validation.help_text()
                        }
            if errors:
                raise ClientRequestValidationException("The requested parameters are not valid", errors=errors)
            request.qwargs = qwargs
            return f(*args, **kwargs)
        return validate_qwargs
    return validate
