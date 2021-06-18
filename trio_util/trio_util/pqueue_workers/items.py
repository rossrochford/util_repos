
class ControlItem(object):
    def __init__(self, line_id=None, work_type=None, flush_group=None, exit=None):
        self.line_id = line_id
        self.work_type = work_type
        self.flush_group = flush_group
        self.exit = exit

    @staticmethod
    def create_from_dict(msg_di):
        work_type = msg_di.get('work_type')
        line_id = msg_di.get('line_id')
        flush_group = msg_di.get('flush_group')
        exit = msg_di.get('exit')

        if not (flush_group or exit):
            return False, None

        if flush_group:
            if not work_type:
                return False, None
        # note: line_id is not mandatory for flush/exit because this could
        # be generated internally rather than from a redis stream
        return True, ControlItem(**msg_di)
