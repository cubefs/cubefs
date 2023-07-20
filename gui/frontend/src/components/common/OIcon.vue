<script>
export default {
  name: 'OIcon',
  functional: true,
  render(h, context) {
    return (
      <i
        class={[
          'o-icon',
          `ion-${context.props.type}`,
          context.data.staticClass
        ]}
      ></i>
    )
  }
}
</script>
