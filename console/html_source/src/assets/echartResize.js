const echartResize = {
  doResize: function (e) {
    const ele = e.target || e.srcElement
    const trigger = ele.__resizeTrigger__
    if (trigger) {
      let handlers = trigger.__z_resizeListeners
      if (handlers) {
        for (let i = 0; i < handlers.length; i++) {
          let h = handlers[i]
          let handler = h.handler
          let context = h.context
          handler.apply(context, [e])
        }
      }
    }
  },
  remove: function (ele, handler, context) {
    const handlers = ele.__z_resizeListeners
    if (handlers) {
      let size = handlers.length
      for (let i = 0; i < size; i++) {
        let h = handlers[i]
        if (h.handler === handler && h.context === context) {
          handlers.splice(i, 1)
          return
        }
      }
    }
  },
  create: function (ele) {
    const obj = document.createElement('object')
    obj.setAttribute('style',
      'display: block; position: absolute; top: 0; left: 0; height: 100%; width: 100%; overflow: hidden;opacity: 0; pointer-events: none; z-index: -1;')
    obj.onload = echartResize.load
    obj.type = 'text/html'
    ele.appendChild(obj)
    obj.data = 'about:blank'
    return obj
  },
  load: function (evt) {
    this.contentDocument.defaultView.__resizeTrigger__ = this.__resizeElement__
    this.contentDocument.defaultView.addEventListener('resize', echartResize.doResize)
  }
}
if (document.attachEvent) { // ie9-10
  echartResize.on = function (ele, handler, context) {
    let handlers = ele.__z_resizeListeners
    if (!handlers) {
      handlers = []
      ele.__z_resizeListeners = handlers
      ele.__resizeTrigger__ = ele
      ele.attachEvent('onresize', echartResize.doResize)
    }
    handlers.push({
      handler: handler,
      context: context
    })
  }
  echartResize.off = function (ele, handler, context) {
    let handlers = ele.__z_resizeListeners
    if (handlers) {
      echartResize.remove(ele, handler, context)
      if (handlers.length === 0) {
        ele.detachEvent('onresize', echartResize.doResize)
        delete ele.__z_resizeListeners
      }
    }
  }
} else {
  echartResize.on = function (ele, handler, context) {
    let handlers = ele.__z_resizeListeners
    if (!handlers) {
      handlers = []
      ele.__z_resizeListeners = handlers

      if (getComputedStyle(ele, null).position === 'static') {
        ele.style.position = 'relative'
      }
      let obj = echartResize.create(ele)
      ele.__resizeTrigger__ = obj
      obj.__resizeElement__ = ele
    }
    handlers.push({
      handler: handler,
      context: context
    })
  }
  echartResize.off = function (ele, handler, context) {
    let handlers = ele.__z_resizeListeners
    if (handlers) {
      echartResize.remove(ele, handler, context)
      if (handlers.length === 0) {
        let trigger = ele.__resizeTrigger__
        if (trigger) {
          trigger.contentDocument.defaultView.removeEventListener('resize', echartResize.doResize)
          ele.removeChild(trigger)
          delete ele.__resizeTrigger__
        }
        delete ele.__z_resizeListeners
      }
    }
  }
}
export {echartResize}
