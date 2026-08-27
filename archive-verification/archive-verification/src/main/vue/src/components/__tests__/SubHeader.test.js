import {describe, it, expect} from 'vitest'
import {mount} from '@vue/test-utils'
import SubHeader from '../SubHeader.vue'

const text = (props) => mount(SubHeader, {props}).get('h4').text()

describe('SubHeader', () => {
  it('defaults the verb to Editing, which is what 16 of 19 pages want', () => {
    expect(text({value: 'Tributaries'})).toBe('Editing: Tributaries')
  })

  it('takes an explicit verb for the read-only pages', () => {
    expect(text({verb: 'Viewing', value: 'FewsLocations'})).toBe('Viewing: FewsLocations')
  })

  // The two IsOriginal pages are keyed on a boolean column. `false` is falsy, so
  // any truthiness test in the template would silently drop it - the row would
  // read "Editing:" whether you had selected the true row or the false one.
  it('renders a false value rather than dropping it', () => {
    expect(text({value: false})).toBe('Editing: false')
  })

  it('renders a true value', () => {
    expect(text({value: true})).toBe('Editing: true')
  })

  it('shows just the verb and colon before anything is selected', () => {
    expect(text({})).toBe('Editing:')
  })
})
