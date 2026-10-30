import {describe, it, expect} from 'vitest'
import {mount} from '@vue/test-utils'
import PageHeader from '../PageHeader.vue'

describe('PageHeader', () => {
  it('renders the title as the page heading', () => {
    const w = mount(PageHeader, {props: {title: 'Class Editor'}})
    expect(w.get('h3').text()).toBe('Class Editor')
  })
})
