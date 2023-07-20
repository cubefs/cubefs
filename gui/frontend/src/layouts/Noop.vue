<script>
export default {
  functional: true,
  render(h, { data, children }) {
    return h('router-view', data, children)
  },
}
</script>
