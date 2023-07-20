<template>
  <div class="cloud-common-skeleton">
    <div class="cloud-common-skeleton_overlay"></div>
    <div class="cloud-common-skeleton_cogs">
      <div class="cloud-common-skeleton_cogs__top">
        <div class="top_part"></div>
        <div class="top_part"></div>
        <div class="top_part"></div>
        <div class="top_hole"></div>
      </div>
      <div class="cloud-common-skeleton_cogs__left">
        <div class="left_part"></div>
        <div class="left_part"></div>
        <div class="left_part"></div>
        <div class="left_hole"></div>
      </div>
      <div class="cloud-common-skeleton_cogs__bottom">
        <div class="bottom_part"></div>
        <div class="bottom_part"></div>
        <div class="bottom_part"></div>
        <div class="bottom_hole">
          <!-- lol -->
        </div>
      </div>
      <p class="loading">loading...</p>
    </div>
  </div>
</template>
<style lang="scss" scoped>
/* Variables */

$pink: #2fc29b;
$blue: #2db7f5;
$yellow: #ff9900;
$font: 'Montserrat', sans-serif;
$heading: rgb(87, 110, 129);
$sub: #2fc29b;

/* Mixins */

@mixin center {
  position: absolute;
  left: 0;
  right: 0;
  top: 0;
  bottom: 0;
  margin: auto;
}

@mixin hole {
  border-radius: 100%;
  background: white;
  position: absolute;
}

p.loading {
  margin: 0 auto;
  text-align: center;
  color: #333;
  font-size: 32px;
}
.cloud-common-skeleton {
  height: 100%;
  position: relative;
  margin: auto;
  width: 100%;
  overflow: hidden;
  &_overlay {
    width: 150px;
    height: 150px;
    background: transparent;
    box-shadow: 0px 0px 0px 1000px rgba(255, 255, 255, 0.67),
      0px 0px 19px 0px rgba(0, 0, 0, 0.16) inset;
    border-radius: 100%;
    z-index: -1;
    @include center;
  }
  &_cogs {
    z-index: -2;
    width: 100px;
    height: 100px;
    top: -120px !important;
    @include center;
    &__top {
      position: relative;
      width: 100px;
      height: 100px;
      transform-origin: 50px 50px;
      animation: rotate 10s infinite linear;
      @for $i from 1 through 3 {
        div:nth-of-type(#{$i}) {
          transform: rotate($i * 30deg);
        }
      }
      div.top_part {
        width: 100px;
        border-radius: 10px;
        position: absolute;
        height: 100px;
        background: $pink;
      }
      div.top_hole {
        width: 50px;
        height: 50px;
        @include hole;
        @include center;
      }
    }
    &__left {
      position: relative;
      width: 80px;
      transform: rotate(16deg);
      top: 28px;
      transform-origin: 40px 40px;
      animation: rotate_left 10s 0.1s infinite reverse linear;
      left: -24px;
      height: 80px;
      @for $i from 1 through 3 {
        div:nth-of-type(#{$i}) {
          transform: rotate($i * 30deg);
        }
      }
      div.left_part {
        width: 80px;
        border-radius: 6px;
        position: absolute;
        height: 80px;
        background: $blue;
      }
      div.left_hole {
        width: 40px;
        height: 40px;
        @include hole;
        @include center;
      }
    }
    &__bottom {
      position: relative;
      width: 60px;
      top: -65px;
      transform-origin: 30px 30px;
      animation: rotate_left 10.2s 0.4s infinite linear;
      transform: rotate(4deg);
      left: 79px;
      height: 60px;
      @for $i from 1 through 3 {
        div:nth-of-type(#{$i}) {
          transform: rotate($i * 30deg);
        }
      }
      div.bottom_part {
        width: 60px;
        border-radius: 5px;
        position: absolute;
        height: 60px;
        background: $yellow;
      }
      div.bottom_hole {
        width: 30px;
        height: 30px;
        @include hole;
        @include center;
      }
    }
  }
}

/* Animations */

@keyframes rotate {
  from {
    transform: rotate(0deg);
  }
  to {
    transform: rotate(360deg);
  }
}

@keyframes rotate_left {
  from {
    transform: rotate(16deg);
  }
  to {
    transform: rotate(376deg);
  }
}

@keyframes rotate_right {
  from {
    transform: rotate(4deg);
  }
  to {
    transform: rotate(364deg);
  }
}
</style>
